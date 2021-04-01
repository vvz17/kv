//============================================================================
// Name        : kv.cpp
// Author      : vvz17
// Version     :
// Copyright   : 
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <boost/version.hpp>
#include <iostream>
#include <sstream>
#include <map>


using namespace std;

const string INSERT = "INSERT";
const string UPDATE = "UPDATE";
const string DELETE = "DELETE";
const string GET = "GET";
const char delim = ',';

typedef std::string kv_key_t;
typedef std::string kv_value_t;
typedef std::map<kv_key_t,kv_value_t> kv_t;

kv_t kv;

struct cmd_t
{
	string     name;
	kv_key_t   key;
	kv_value_t value;
	cmd_t(const string& line)
	{
		std::stringstream ss(line);
		std::string buf;
		std::getline(ss,buf,delim);
		name = buf;
		std::getline(ss,buf,delim);
		key = buf;
		std::getline(ss,buf,delim);
		value = buf;
	}

	void handle() {

		if(name == INSERT)
		{
//			std::clog << "start handling " << INSERT << std::endl;
			auto it = kv.find(key);
			if(it != kv.end())
			{
				std::cerr << name << " ERROR key exists: " << key << std::endl;
				return;
			}

			kv.insert(kv.begin(),std::make_pair(key,value));

			std::clog << name << " OK new key-value: " << key << '=' << kv[key] << std::endl;
		}
		else if(name == UPDATE)
		{
//			std::clog << "start handling " << UPDATE << std::endl;
			auto it = kv.find(key);
			if(it == kv.end())
			{
				std::cerr << name << " ERROR key not found: " << key << std::endl;
				return;
			}
			if(it->second == value)
			{
				std::cerr << name << " ERROR value has not been changed: " << key << '=' << kv[key] << std::endl;
				return;
			}

			it->second = value;

			std::clog << name << " OK new value: " << key << '=' << kv[key] << std::endl;
		}
		else if(name == DELETE)
		{
//			std::clog << "start handling " << DELETE << std::endl;
			auto it = kv.find(key);
			if(it == kv.end())
			{
				std::cerr << name << " ERROR key not found: " << key << std::endl;
				return;
			}

			kv.erase(it);

			std::clog << name << " OK key #count: " << key << "#" << kv.count(key) << std::endl;
		}
		else if(name == GET)
		{
//			std::clog << "start handling " << GET << std::endl;
			auto it = kv.find(key);
			if(it == kv.end())
			{
				std::cerr << name << " ERROR key not found: " << key << std::endl;
				return;
			}

			std::clog << name << " OK key found: " << key << '=' << it->second << std::endl;
		}
		else
		{
			std::cerr << name << " ERROR unknown command" << std::endl;
		}
	}
};

int main() {
	clog << "boost version:" <<BOOST_LIB_VERSION<< endl;
	clog << "KV" << endl; // prints KV

	string line;
	while(std::getline(cin, line))
	{
		cmd_t cmd = line;
		std::clog << line << std::endl;
		cmd.handle();
	}
	return 0;
}
