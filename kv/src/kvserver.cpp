//============================================================================
// Name        : kvserver.cpp
// Author      : Василий Зазнобин
// Version     : 1.0
// Description : Реализация небольшой key-value базы данных.

// Cервер слушает запросы на указанном IP:PORT и предоставляет API для простой key-value базы данных.
// Внутреннее хранилище сервера - boost::multi_index, который хранится в маппируемом в оперативную память файле.
// Ключ имеет тип - Строка (максимальная длина 1024 символа).
// Значение имеет тип - Строка (максимальная длина 1024 * 1024 символа).

// База данных поддерживает операции:
//    INSERT - добавить key:value;
//    UPDATE - изменить key:value;
//    DELETE - удалить key;
//    GET - получить value по key.

// Если ключ уже существует, то при операции INSERT База Данных возвращает ошибку, что запись отсутсвует.
// Если ключ не существует, то при операции UPDATE База Данных возвращает ошибку, что запись отсутсвует.
// Если ключ существует и значение совпадает, то при операции UPDATE База Данных возвращает ошибку, что значение не было изменено.
// Если ключ не существует, то при операции DELETE База Данных возвращает ошибку об отсутствующей записи.
// Если ключ не существует, то при операции GET База Данных возвращает ошибку об отсутствующей записи.

// Сервер ведет статистику отправленных и полученных комманд.
// С переодичности в 60 секунд, сервер выводит на std::cerr статистику:
//    - количество записей в БД;
//    - количество успешных/неуспешных операций INSERT;
//    - количество успешных/неуспешных операций UPDATE;
//    - количество успешных/неуспешных операций DELETE;
//    - количество успешных/неуспешных операций GET.

// Общение по собственному протоколу поверх TCP/IP.
// Для автоматизации сериализации структур использовать boost::fusion (не реализовано).
// Для реализации сетевого взаимодействия использовать boost::asio.
// Для реализации потоков использовать возможности boost::asio.
// Для реализации таймеров использовать возможности boost::asio.
// Для хранения данных использовать boost::multi_index и boost::interprocess.
//============================================================================

#include <iostream>
#include <string>
#include <vector>
#include <iterator>
#include <algorithm>
#include <functional>
#include <sstream>
#include <memory>
#include <utility>
#include <thread>

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/make_shared.hpp>
#include <boost/utility.hpp>

#include <boost/version.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read_until.hpp>

constexpr const char* INSERT = "INSERT";
constexpr const char* UPDATE = "UPDATE";
constexpr const char* DELETE = "DELETE";
constexpr const char* GET = "GET";
constexpr char delim = ',';

namespace bip = boost::interprocess;
using mutex_type    = bip::named_mutex;

template <typename T> using shared_alloc  = bip::allocator<T, bip::managed_mapped_file::segment_manager>;
template <typename T> using shared_vector = boost::container::vector<T, shared_alloc<T> >;
template <typename K, typename V, typename P = std::pair<K,V>, typename Cmp = std::less<K> >
                      using shared_map    = boost::container::flat_map<K, V, Cmp, shared_alloc<P> >;

namespace bmi = boost::multi_index;

template<typename T,typename IndexSpecifierList>
					using shared_multi_index = bmi::multi_index_container<T, IndexSpecifierList, shared_alloc<T> >;

using shared_string = bip::basic_string<char, std::char_traits<char>, shared_alloc<char> >;
using kv_t = shared_map<shared_string, shared_string>;

struct key_value_t
{
	shared_string   key;
	shared_string 	value;

	struct by_key {}; struct by_value {};

	key_value_t(const shared_alloc<char>& alloc): key(alloc), value(alloc)
	{

	}
};

using multi_index_kv_t =
	shared_multi_index<key_value_t,
		bmi::indexed_by<
			bmi::hashed_unique< bmi::tag<key_value_t::by_key>, bmi::member<key_value_t, shared_string, &key_value_t::key> >,
			bmi::hashed_non_unique< bmi::tag<key_value_t::by_value>, bmi::member<key_value_t, shared_string, &key_value_t::value> >
		>
	>;

struct mutex_remove
{
    mutex_remove() { mutex_type::remove("d7089fba-9476-11eb-a840-43724187bbab"); }
    ~mutex_remove(){ mutex_type::remove("d7089fba-9476-11eb-a840-43724187bbab"); }
} remover;

static mutex_type mutex(bip::open_or_create,"d7089fba-9476-11eb-a840-43724187bbab");

static kv_t& shared_instance()
{
    bip::scoped_lock<mutex_type> lock(mutex);
    static bip::managed_mapped_file seg(bip::open_or_create,"./kv.db", 50ul<<20); // "50Mb ought to be enough for anyone"

    static kv_t* _instance = seg.find_or_construct<kv_t>
        ("kv_t")
        (
         std::less<shared_string>(),
         kv_t::allocator_type(seg.get_segment_manager())
        );

//    static auto capacity = seg.get_free_memory();
//    static auto size = seg.get_size();
//    std::clog << "KV Free space: " << (capacity>>20) << " Size:" << (size>>20) << "Mb\n";

    return *_instance;
}

static multi_index_kv_t& shared_instance2()
{
    bip::scoped_lock<mutex_type> lock(mutex);
    static bip::managed_mapped_file seg(bip::open_or_create,"./kv2.db", 50ul<<20); // "50Mb ought to be enough for anyone"

    static multi_index_kv_t* _instance = seg.find_or_construct<multi_index_kv_t>
        ("multi_index_kv_t")
        (
        	multi_index_kv_t::allocator_type(seg.get_segment_manager())
        );

//    static auto capacity = seg.get_free_memory();
//    static auto size = seg.get_size();
//    std::clog << "KV2 Free space: " << (capacity>>20) << " Size:" << (size>>20) << "Mb\n";

    return *_instance;
}

struct stat_t
{
	std::pair<int,int> INSERT;
	std::pair<int,int> UPDATE;
	std::pair<int,int> DELETE;
	std::pair<int,int> GET;
};

template<typename KV>
struct cmd_t : public key_value_t
{
	std::string name;
	KV& kv;

	cmd_t(KV& kv_, const std::string& line)
	: key_value_t(kv_.get_allocator().get_segment_manager())
	, name()
	, kv(kv_)
	{
		std::stringstream ss(line);
		std::string buf;

		std::getline(ss,buf,delim);
		name = buf;

		std::getline(ss,buf,delim);
		key.assign(buf.begin(), buf.end());

		std::getline(ss,buf,delim);
		value.assign(buf.begin(), buf.end());
	}

	std::string handle(stat_t&);
};

template<>
	std::string cmd_t<multi_index_kv_t>::handle(stat_t& stat)
	{
		std::stringstream ss;

		if(name == INSERT)
		{
			auto &index = kv.get<key_value_t::by_key>();
			auto it = index.find(key);
			if(it != index.end())
			{
				ss << name << " ERROR key exists: " << key << std::endl;
				++stat.INSERT.second;
				return ss.str();
			}

			auto it2 = kv.insert(*this);
			if(it2.second)
			{
				ss << name << " OK new key-value: " << (*it2.first).key << '=' << (*it2.first).value << std::endl;
				++stat.INSERT.first;
				return ss.str();
			}
		}
		else if(name == UPDATE)
		{
			auto &index = kv.get<key_value_t::by_key>();
			auto it = index.find(key);
			if(it == index.end())
			{
				ss << name << " ERROR key not found: " << key << std::endl;
				++stat.UPDATE.second;
				return ss.str();
			}
			if((*it).value == value)
			{
				ss << name << " ERROR value has not been changed: " << key << '=' << (*it).value << std::endl;
				++stat.UPDATE.second;
				return ss.str();
			}

			index.modify(it, [this](key_value_t &e){ e.value = value; });
			ss << name << " OK new value: " << (*it).key << '=' << (*it).value << std::endl;
			++stat.UPDATE.first;
			return ss.str();
		}
		else if(name == DELETE)
		{
			auto &index = kv.get<key_value_t::by_key>();
			auto it = index.find(key);
			if(it == index.end())
			{
				ss << name << " ERROR key not found: " << key << std::endl;
				++stat.DELETE.second;
				return ss.str();
			}

			index.erase(it);

			ss << name << " OK key #count: " << key << "#" << kv.count(key) << std::endl;
			++stat.DELETE.first;
			return ss.str();
		}
		else if(name == GET)
		{
			auto &index = kv.get<key_value_t::by_key>();
			auto it = index.find(key);
			if(it == index.end())
			{
				ss << name << " ERROR key not found: " << key << std::endl;
				++stat.GET.second;
				return ss.str();
			}

			ss << name << " OK key found: " << key << '=' << (*it).value << std::endl;
			++stat.GET.first;
			return ss.str();
		}
		else
		{
			ss << name << " ERROR unknown command" << std::endl;
			++stat.GET.second;
			return ss.str();
		}
		return ss.str();
	}

template<>
	std::string cmd_t<kv_t>::handle(stat_t&)
	{
		std::stringstream ss;
		if(name == INSERT)
		{
//			std::clog << "start handling " << INSERT << std::endl;

			auto it = kv.find(key);
			if(it != kv.end())
			{
				ss << name << " ERROR key exists: " << key << std::endl;
				return ss.str();
			}

			auto it2 = kv.insert(kv.begin(),std::make_pair(key,value));

			ss << name << " OK new key-value: " << key << '=' << it2->second << std::endl;
			return ss.str();
		}
		else if(name == UPDATE)
		{
//			std::clog << "start handling " << UPDATE << std::endl;

			auto it = kv.find(key);
			if(it == kv.end())
			{
				ss << name << " ERROR key not found: " << key << std::endl;
				return ss.str();
			}
			if(it->second == value)
			{
				ss << name << " ERROR value has not been changed: " << key << '=' << it->second << std::endl;
				return ss.str();
			}

			it->second = value;

			ss << name << " OK new value: " << key << '=' << it->second << std::endl;
			return ss.str();
		}
		else if(name == DELETE)
		{
//			std::clog << "start handling " << DELETE << std::endl;
			auto it = kv.find(key);
			if(it == kv.end())
			{
				ss << name << " ERROR key not found: " << key << std::endl;
				return ss.str();
			}

			kv.erase(it);

			ss << name << " OK key #count: " << key << "#" << kv.count(key) << std::endl;
			return ss.str();
		}
		else if(name == GET)
		{
//			std::clog << "start handling " << GET << std::endl;
			auto it = kv.find(key);
			if(it == kv.end())
			{
				ss << name << " ERROR key not found: " << key << std::endl;
				return ss.str();
			}

			ss << name << " OK key found: " << key << '=' << it->second << std::endl;
			return ss.str();
		}
		else
		{
			ss << name << " ERROR unknown command" << std::endl;
			return ss.str();
		}
		return ss.str();
	}

template<typename KV>
	cmd_t<KV> parse_cmd(KV& kv, const std::string& line)
	{
		return cmd_t<KV>(kv,line);
	}

using boost::asio::ip::tcp;

template<typename KV>
class session
{
public:
	session( KV& kv
			, boost::asio::io_service& io_service
			, stat_t& stat
			, boost::shared_mutex& mx)
	: kv_(kv)
	, socket_(io_service)
	, stat_(stat)
	, mx_(mx)
	{
	}

	tcp::socket& socket()
	{
		return socket_;
	}

	void start()
	{
		boost::asio::async_read_until(socket_, receive_data, "\n",
				boost::bind(&session<KV>::handle_read, this,
						boost::asio::placeholders::error,
						boost::asio::placeholders::bytes_transferred));
	}

private:
	void handle_read(const boost::system::error_code& error,
			size_t bytes_transferred)
	{
		if (!error)
		{
			std::string line;
			std::istream is(&receive_data);
			std::getline(is, line);

			auto cmd = parse_cmd(kv_,line);
			std::string answer;

			{
				boost::lock_guard<boost::shared_mutex> guard(mx_);
				answer = cmd.handle(stat_);
			}

			std::ostream os(&send_data);
			os << answer << std::endl;

			boost::asio::async_write(socket_,
					send_data.data(),
					boost::bind(&session<KV>::handle_write, this,
							boost::asio::placeholders::error));
		}
		else
		{
			delete this;
		}
	}

	void handle_write(const boost::system::error_code& error)
	{
		if (!error)
		{
			boost::asio::async_read_until(socket_, receive_data, "\n",
					boost::bind(&session<KV>::handle_read, this,
							boost::asio::placeholders::error,
							boost::asio::placeholders::bytes_transferred));
		}
		else
		{
			delete this;
		}
	}

	KV& kv_;
	tcp::socket socket_;
	stat_t& stat_;
	boost::shared_mutex& mx_;
	boost::asio::streambuf send_data;
	boost::asio::streambuf receive_data;
};

template<typename KV>
class server
{
public:
	server(KV& kv, boost::asio::io_service& io_service, short port, long seconds_on_time_statistics)
	: kv_(kv)
	, io_service_(io_service)
	, acceptor_(io_service, tcp::endpoint(tcp::v4(), port))
	, interval(seconds_on_time_statistics)
	, timer(io_service, interval)
	{
		start_accept();
	}
	void run()
	{
	  std::vector<boost::shared_ptr<std::thread> > threads;
	  for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i)
	  {
	    boost::shared_ptr<std::thread> thread(new std::thread(boost::bind(&boost::asio::io_service::run, &io_service_)));
	    threads.push_back(thread);
	  }

	  timer.async_wait(boost::bind(&server<KV>::on_time_statistic, this, _1));

	  for (std::size_t i = 0; i < threads.size(); ++i)
	    threads[i]->join();
	}
private:
	void start_accept()
	{
		session<KV>* new_session = new session<KV>(kv_,io_service_,stat_,mx_);
		acceptor_.async_accept(new_session->socket(),
				boost::bind(&server::handle_accept, this, new_session,
						boost::asio::placeholders::error));
	}

	void handle_accept(session<KV>* new_session,
			const boost::system::error_code& error)
	{
		if (!error)
		{
			new_session->start();
		}
		else
		{
			delete new_session;
		}

		start_accept();
	}
	void on_time_statistic(const boost::system::error_code& /*e*/) {

	    boost::lock_guard<boost::shared_mutex> guard(mx_);

	    std::cout << "------" << std::endl;
	    std::cout << "count  " << kv_.size() << std::endl;
	    std::cout << "INSERT " << stat_.INSERT.first << "/" << stat_.INSERT.second << std::endl;
	    std::cout << "UPDATE " << stat_.UPDATE.first << "/" << stat_.UPDATE.second << std::endl;
	    std::cout << "DELETE " << stat_.DELETE.first << "/" << stat_.DELETE.second << std::endl;
	    std::cout << "GET    " << stat_.GET.first << "/" << stat_.GET.second << std::endl;

	    timer.expires_at(timer.expires_at() + interval);
	    timer.async_wait(boost::bind(&server<KV>::on_time_statistic, this, _1));
	}

	KV& kv_;
	boost::asio::io_service& io_service_;
	tcp::acceptor acceptor_;
	boost::posix_time::seconds interval;
	boost::asio::deadline_timer timer;
	stat_t stat_;
	boost::shared_mutex mx_;
};

int main(int argc, char* argv[]) {

	try
	{
		std::cout << "kvserver" << std::endl;

		if (argc != 2) {
			std::cerr << "Usage: kvserver <port>\n";
			return 1;
		}

//		auto& kv = shared_instance();
		auto& kv = shared_instance2();

		bip::scoped_lock<mutex_type> lock(mutex);

		boost::asio::io_service io_service;
		server<decltype(kv)> s(kv, io_service, std::atoi(argv[1]), 60);

		s.run();
	}
	catch (std::exception& e)
	{
		std::cerr << "Exception: " << e.what() << "\n";
	}

	return 0;
}
