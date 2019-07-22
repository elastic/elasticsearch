package org.elasticsearch.graphql.api;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContentGenerator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GqlMockApi implements GqlApi {
    /*
    @Override
    public Map<String, Object> getHello() {
        LinkedHashMap<String, Object> result = new LinkedHashMap();
        result.put("name", "some-name");
        result.put("cluster_name", "elasticsearch");
        result.put("cluster_uuid", "1-D8qMuTS6qhoRIIUIq4Vw");

        LinkedHashMap<String, Object> version = new LinkedHashMap();
        version.put("number", "8.0.0-SNAPSHOT");
        version.put("build_flavor", "default");
        version.put("build_type", "tar");
        version.put("build_hash", "2aebbe6");
        version.put("build_date", "2019-07-21T16:10:22.420795Z");
        version.put("build_snapshot", true);
        version.put("lucene_version", "8.2.0");
        version.put("minimum_wire_compatibility_version", "7.4.0");
        version.put("minimum_index_compatibility_version", "7.0.0");
        result.put("version", version);

        result.put("tagline", "You Know, for Search");

        return result;
    }
    */

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Object>> getHello() throws Exception {
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();
        builder.startObject();
        builder.field("name", "some-name");
        builder.field("cluster_name", "elasticsearch");
        builder.field("cluster_uuid", "1-D8qMuTS6qhoRIIUIq4Vw");
        builder.startObject("version")
            .field("number", "8.0.0-SNAPSHOT")
            .field("build_flavor", "default")
            .field("build_type", "tar")
            .field("build_hash", "2aebbe6")
            .field("build_date", "2019-07-21T16:10:22.420795Z")
            .field("build_snapshot", true)
            .field("lucene_version", "8.2.0")
            .field("minimum_wire_compatibility_version", "7.4.0")
            .field("minimum_index_compatibility_version", "7.0.0")
            .endObject();
        builder.field("tagline", "You Know, for Search");
        builder.endObject();


        CompletableFuture<Map<String, Object>> promise = new CompletableFuture<>();
        promise.complete((Map) ((JavaUtilXContentGenerator) builder.generator()).getResult());
        return promise;
    }
}
