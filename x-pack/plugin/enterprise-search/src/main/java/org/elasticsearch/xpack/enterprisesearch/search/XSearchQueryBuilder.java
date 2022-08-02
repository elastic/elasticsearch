/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.search;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;

// It would be interesting if this implemented AbstractQueryBuilder here
public class XSearchQueryBuilder {

    public static QueryBuilder getQueryBuilder(XSearchQueryOptions queryOptions) {
        QueryBuilder multiMatchQueryOther = QueryBuilders
            .multiMatchQuery(
                queryOptions.getQuery(),
                "world_heritage_site^1.0",
                "world_heritage_site.stem^0.95",
                "world_heritage_site.prefix^0.1",
                "world_heritage_site.joined^0.75",
                "world_heritage_site.delimiter^0.4",
                "description^2.4",
                "description.stem^2.28",
                "description.prefix^0.24",
                "description.joined^1.8",
                "description.delimiter^0.96",
                "title^5.0",
                "title.stem^4.75",
                "title.prefix^0.5",
                "title.joined^3.75",
                "title.delimiter^2.0",
                "nps_link^0.7",
                "nps_link.stem^0.665",
                "nps_link.prefix^0.07",
                "nps_link.joined^0.525",
                "nps_link.delimiter^0.28",
                "states^2.8",
                "states.stem^2.66",
                "states.prefix^0.28",
                "states.joined^2.1",
                "states.delimiter^1.12",
                "id^1.0")
            .minimumShouldMatch("1<-1 3<49%")
            .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);

        QueryBuilder stemMultiMatch = QueryBuilders
            .multiMatchQuery(
                queryOptions.getQuery(),
                "world_heritage_site.stem^0.1",
                "description.stem^0.24",
                "title.stem^0.5",
                "nps_link.stem^0.07",
                "states.stem^0.28")
            .minimumShouldMatch("1<-1 3<49%")
            .fuzziness(Fuzziness.AUTO)
            .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .prefixLength(2);

        QueryBuilder multiMatchQuery = QueryBuilders
            .multiMatchQuery(
                queryOptions.getQuery(),
                queryOptions.getFieldNames()
            )
            .minimumShouldMatch("1<-1 3<49%")
            .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);

        return QueryBuilders.boolQuery().should(multiMatchQuery);
    }
}
