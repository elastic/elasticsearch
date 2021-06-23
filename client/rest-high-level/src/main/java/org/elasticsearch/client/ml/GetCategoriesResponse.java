/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.common.xcontent.ParseField;
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
