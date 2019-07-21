package org.elasticsearch.graphql.api;

import java.util.LinkedHashMap;
import java.util.Map;

public class GqlElasticsearchApi implements GqlApi {
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
}
