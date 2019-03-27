package com.itcast;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchTest {

    /**
     * 创建索引
     */
    @Test
    public void testSaveToIndex() throws IOException {
        //创建客户端访问对象
        /**
         * Settings表示集群的设置
         * EMPTY：表示没有集群的配置
         * Settings.EMPTY
         * 9300是tcp通讯端口,java程序调用的时候使用
         * 9200是http协议的RESTful接口
         */
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));// 需要指定ES的地址和端口

        //创建文档对象
        //组织Document数据（使用ES的api构建json）
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id",3)
                .field("title","3--ElasticSearch是一个基于Lucene的搜索服务器。")
                .field("content","3--它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，" +
                        "是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();
        //创建索引，创建文档类型，设置唯一主键，同时创建文档
        client.prepareIndex("blog","article","3").setSource(builder).get();

        //关闭资源
        client.close();
    }

    /**
     * 根据id查询文档
     */
    @Test
    public void testQueryById() throws UnknownHostException {
        //创建client对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));// 需要指定ES的地址和端口

        //获取文档
        GetResponse getResponse = client.prepareGet("blog","article","3").get();
        String source = getResponse.getSourceAsString();
        System.out.println(source);
        //关闭资源
        client.close();
    }

    //查询全部（不走索引）
    @Test
    public void testFindAll() throws UnknownHostException {
        //创建客户端访问对象client对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //设置查询条件（QueryBuilders.matchAllQuery()：查询所有）
        SearchResponse response = client.prepareSearch("blog")  //根据索引查询
                .setTypes("article").setQuery(QueryBuilders.matchAllQuery()).get();

        SearchHits hits = response.getHits();
        System.out.println("共查询到的数目："+hits.getTotalHits());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());//打印每一条数据
            System.out.println(hit.getSource().get("title"));//打印每一条数据中的title属性
        }
        //关闭资源
        client.close();
    }

    //字符串查询
    @Test
    public void testQueryString() throws UnknownHostException {
        //创建客户端访问对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //设置查询条件
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("搜索").field("title").field("content"))//在title和content字段中搜索名字为“搜索”的所有内容
                .get();
        //处理结果
        SearchHits hits = response.getHits();//获得命中目标，获得了多少个对象
        System.out.println("总共获得数量："+hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getSource().get("title"));
        }
        //关闭资源
        client.close();
    }

    //词条查询
    @Test
    public void testQueryWord() throws UnknownHostException {
        //创建客户端访问对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //设置查询条件
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("title","搜索")).get();

        //处理结果
        SearchHits hits = response.getHits();//获得命中目标，获得了多少个对象
        System.out.println("总共获得数量："+hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getSource().get("title"));
        }
        //关闭资源
        client.close();
    }

    //通配符模糊查询
    @Test
    public void testQuery() throws UnknownHostException {
        //创建客户端访问对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //设置查询条件
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("title","*搜索*")).get();

        //处理结果
        SearchHits hits = response.getHits();//获得命中目标，获得了多少个对象
        System.out.println("总共获得数量："+hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getSource().get("title"));
        }
        //关闭资源
        client.close();
    }
}
