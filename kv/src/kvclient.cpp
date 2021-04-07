//============================================================================
// Name        : kvclient.cpp
// Author      : Василий Зазнобин
// Version     : 1.0
// Description : Реализация небольшого клиента к key-value базы данных.

// Клиент получает из коммандной строки:
//   - адрес сервера;
//   - команду;
//   - ключ;
//   - значение.
//============================================================================

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

enum { max_length = 1024 };

int main(int argc, char* argv[])
{
	try
	{
		if (argc != 4)
		{
			std::cerr << "Usage: kvclient <host> <port> <cmd,key,value>\n";
			return 1;
		}

		boost::asio::io_service io_service;

		tcp::socket s(io_service);
		tcp::resolver resolver(io_service);
		boost::asio::connect(s, resolver.resolve({argv[1], argv[2]}));

		boost::asio::streambuf send_data;
		boost::asio::streambuf receive_data;

		std::string cmd(argv[3]);

		std::ostream os(&send_data);
		os << cmd << std::endl;

		boost::asio::write(s, send_data);

		boost::asio::read_until(s, receive_data, "\n");
		std::string line;
		std::istream is(&receive_data);
		std::getline(is, line);

		std::cout << line << std::endl;
	}
	catch (std::exception& e)
	{
		std::cerr << "Exception: " << e.what() << "\n";
	}

	return 0;
}
