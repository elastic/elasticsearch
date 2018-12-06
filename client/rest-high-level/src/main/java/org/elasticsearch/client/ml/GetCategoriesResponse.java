/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A response containing the requested categories
 */
public class GetCategoriesResponse extends AbstractResultResponse<CategoryDefinition> {

    public static final ParseField CATEGORIES = new ParseField("categories");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetCategoriesResponse, Void> PARSER = 
            new ConstructingObjectParser<>("get_categories_response", true,
                    a -> new GetCategoriesResponse((List<CategoryDefinition>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), CategoryDefinition.PARSER, CATEGORIES);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
    }

    public static GetCategoriesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetCategoriesResponse(List<CategoryDefinition> categories, long count) {
        super(CATEGORIES, categories, count);
    }

    /**
     * The retrieved categories
     * @return the retrieved categories
     */
    public List<CategoryDefinition> categories() {
        return results;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, results);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetCategoriesResponse other = (GetCategoriesResponse) obj;
        return count == other.count && Objects.equals(results, other.results);
    }
}
