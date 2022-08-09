#include "basic.hpp"
#include "rocksdb-serializer.hpp"

#include <boost/asio.hpp>

#include <algorithm>
#include <iostream>

#include <memory>
#include <array>
#include <list>
#include <thread>
#include <vector>
#include <random>
#include <chrono>

using boost::asio::ip::tcp;

template<typename Function>
auto record(Function &&f, std::string memo = "") -> long int
{
    //std::chrono::high_resolution_clock::time_point;
    auto const start = std::chrono::high_resolution_clock::now();
    std::invoke(f);
    auto const now = std::chrono::high_resolution_clock::now();
    auto relativetime = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start).count();
    BOOST_LOG_TRIVIAL(info) << memo << ": " << relativetime << "\n";
    return relativetime;
}

int main()
{
    basic::init_log();
    boost::asio::io_context io_context;
    tcp::socket s(io_context);
    tcp::resolver resolver(io_context);
    boost::asio::connect(s, resolver.resolve("localhost", "12000"));

    record([&](){ ; }, "base");

    { // send merge_request_commit;
        rocksdb_pack::packet_pointer ptr = std::make_shared<rocksdb_pack::packet>();
        ptr->header.type = rocksdb_pack::msg_t::merge_request_commit;
        ptr->header.uuid = rocksdb_pack::key_t{
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 9
        };

        ptr->header.blockid = 2;
        ptr->header.position = 20;

        std::uint32_t version = 5;
        version = rocksdb_pack::hton(version);

        ptr->data.buf = std::vector<rocksdb_pack::unit_t> (sizeof(version));
        std::memcpy(ptr->data.buf.data(), &version, sizeof(version));

        auto buf = ptr->serialize();
        boost::asio::write(s, boost::asio::buffer(buf->data(), buf->size()));
    } // send merge_request_commit;

    { // read resp
        rocksdb_pack::packet_pointer resp = std::make_shared<rocksdb_pack::packet>();
        std::vector<rocksdb_pack::unit_t> headerbuf(rocksdb_pack::packet_header::bytesize);

        boost::asio::read(s, boost::asio::buffer(headerbuf.data(), headerbuf.size()));
        resp->header.parse(headerbuf.data());

        BOOST_LOG_TRIVIAL(info) << "put resp " << resp->header;

        std::vector<rocksdb_pack::unit_t> bodybuf(resp->header.datasize);

        boost::asio::read(s, boost::asio::buffer(bodybuf.data(), bodybuf.size()));
        resp->data.parse(resp->header.datasize, bodybuf.data());
    } // read resp


    { // send merge_execute_commit;
        rocksdb_pack::packet_pointer ptr = std::make_shared<rocksdb_pack::packet>();
        ptr->header.type = rocksdb_pack::msg_t::merge_execute_commit;
        ptr->header.uuid = rocksdb_pack::key_t{
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 9
        };

        ptr->header.blockid = 2;
        ptr->header.position = 20;

        std::uint32_t version = 10;
        version = rocksdb_pack::hton(version);

        ptr->data.buf = std::vector<rocksdb_pack::unit_t> {0, 0, 0, 0, 65, 66, 67, 68};
        std::memcpy(ptr->data.buf.data(), &version, sizeof(version));

        auto buf = ptr->serialize();
        boost::asio::write(s, boost::asio::buffer(buf->data(), buf->size()));
    } // send merge_execute_commit;

    { // read resp
        rocksdb_pack::packet_pointer resp = std::make_shared<rocksdb_pack::packet>();
        std::vector<rocksdb_pack::unit_t> headerbuf(rocksdb_pack::packet_header::bytesize);
        boost::asio::read(s, boost::asio::buffer(headerbuf.data(), headerbuf.size()));

        resp->header.parse(headerbuf.data());
    } // read resp

    { // send get
        rocksdb_pack::packet_pointer ptr = std::make_shared<rocksdb_pack::packet>();
        ptr->header.type = rocksdb_pack::msg_t::get;
        ptr->header.uuid = rocksdb_pack::key_t{
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 8,
            7, 8, 7, 8, 7, 8, 7, 9
        };

        ptr->header.blockid = 2;
        ptr->header.position = 1;
        ptr->header.datasize = 2;
        auto buf = ptr->serialize_header();
        boost::asio::write(s, boost::asio::buffer(buf->data(), buf->size()));
    } // send merge_execute_commit;

    { // read resp
        rocksdb_pack::packet_pointer resp = std::make_shared<rocksdb_pack::packet>();
        std::vector<rocksdb_pack::unit_t> headerbuf(rocksdb_pack::packet_header::bytesize);
        boost::asio::read(s, boost::asio::buffer(headerbuf.data(), headerbuf.size()));

        resp->header.parse(headerbuf.data());
        BOOST_LOG_TRIVIAL(debug) << "header " << resp->header << "\n";

        std::string bodybuf(resp->header.datasize, 0);
        boost::asio::read(s, boost::asio::buffer(bodybuf.data(), bodybuf.size()));

        for (int i : bodybuf)
            BOOST_LOG_TRIVIAL(debug) << i << " ";

        BOOST_LOG_TRIVIAL(debug) << bodybuf << "\n";
    } // read resp


    return EXIT_SUCCESS;
}
