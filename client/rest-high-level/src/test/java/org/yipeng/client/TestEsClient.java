package org.yipeng.client;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.yipeng.client.data.DataReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

/**
 * Created by yipeng on 2018/10/6.
 * To build the data  of ware to es
 */
public class TestEsClient {


    public static void main(String[] args) throws IOException {
        putFullIndex();
//        deleteIndex();
//        createIndex();
    }


    public static void putFullIndex() throws IOException {
        RestClientBuilder restClientBuilder = getRestClientBuilder();

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

//        List<Map<String, String>> indexMapDocList = DataReader.getIndexDatas();

        IndexRequest indexRequest = new IndexRequest("wares", "wares", "123"); /**最后会在 {@link org.elasticsearch.action.bulk.TransportShardBulkAction#executeBulkItemRequest}执行**/
        Map<String, Object> map = new HashMap<>();
        map.put("ware_id", 111);
        map.put("brand_id", 22222);
        indexRequest.source(map);


        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        restHighLevelClient.close();
        BulkRequest bulkRequest = new BulkRequest();
    }

    public static void deleteIndex() throws IOException {
        RestClientBuilder restClientBuilder = getRestClientBuilder();

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        restHighLevelClient.indices().delete(new DeleteIndexRequest("wares"), RequestOptions.DEFAULT);
    }

    public static void createIndex() throws IOException {   /**最后在{@link org.elasticsearch.cluster.metadata.MetaDataCreateIndexService.IndexCreationTask#execute(ClusterState)} 执行**/
        RestClientBuilder restClientBuilder = getRestClientBuilder();

        CreateIndexRequest request = new CreateIndexRequest("wares");

        String mapping = Strings.toString(createMapping());
        request.mapping("wares", mapping, XContentType.JSON);   //注意必须设置XcontentType.Json,不然PutMappingRequest.buildFromSimplifiedDef会报错

        request.settings(createSettingBuilder());

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    public static RestClientBuilder getRestClientBuilder() {
        HttpHost httpHost = new HttpHost("127.0.0.1", 9200);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        return restClientBuilder;
    }


    public static XContentBuilder createSettingBuilder() throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("index")
            .field("number_of_shards", 5)
            .field("number_of_replicas", 1)
            .endObject()
            .endObject();

        return settingBuilder;
    }


    public static XContentBuilder createMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("wares")
            .field("dynamic", false)
            .startObject("properties")

            .startObject("ware_id").field("type", "keyword").field("index", "true").
                field("store", "true").field("doc_values", "true").endObject()

            .startObject("category_id").field("type", "long").field("index", "true").
                field("store", "true").field("doc_values", "true").endObject()

            .startObject("vender_id").field("type", "long").field("index", "true").
                field("store", "true").field("doc_values", "true").endObject()

            .startObject("title").field("type", "text").field("index", "true").
                field("store", "true").field("analyzer", "whitespace")
            .field("search_analyzer", "whitespace").endObject()

            .startObject("online_time").field("type", "keyword").field("index", "true").
                field("store", "true").endObject()

            .startObject("stock").field("type", "integer").field("index", "true").
                field("store", "true").field("doc_values", "true").endObject()

            .startObject("brand_id").field("type", "long").field("index", "true").
                field("store", "true").field("doc_values", "true").endObject()


            .startObject("s1").field("type", "float").field("index", "false").
                field("store", "true").field("doc_values", "true").endObject()

            .startObject("s2").field("type", "float").field("index", "false").
                field("store", "true").field("doc_values", "true").endObject()

            .endObject()
            .startObject("_source").field("enabled", "false").endObject()
            .endObject()
            .endObject();

        return builder;

    }

}
