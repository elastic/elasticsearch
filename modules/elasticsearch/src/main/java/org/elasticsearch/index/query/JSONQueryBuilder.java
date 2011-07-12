/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;
/**
 * Created by IntelliJ IDEA.
 * User: cedric
 * Date: 12/07/11
 * Time: 11:30
 */

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.jackson.JsonParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;

import java.io.IOException;

/**
 * A Query builder which allows building a query thanks to a JSON string. This is useful when you want
 * to use the Java Builder API but still have JSON query strings at hand that you want to combine with other
 * query builders.
 *
 * Example usage in a boolean query :
 * <pre>
 * {@code
 *      BoolQueryBuilder bool = new BoolQueryBuilder();
 *      bool.must(new JSONQueryBuilder("{\"term\": {\"field\":\"value\"}}");
 *      bool.must(new TermQueryBuilder("field2","value2");
 * }
 * </pre>
 *
 * @author Cedric Champeau
 */
public class JSONQueryBuilder extends BaseQueryBuilder {

    private final String jsonQuery;

    /**
     * Builds a JSONQueryBuilder using the provided JSON query string.
     * @param jsonQuery
     */
    public JSONQueryBuilder(String jsonQuery) {
        this.jsonQuery = jsonQuery;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSONQueryParser.NAME);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonQuery);
        parser.nextToken();
        builder.field("value");
        builder.copyCurrentStructure(parser);
        builder.endObject();
    }
}
