/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.support;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DLSRoleQueryValidatorTests extends ESTestCase {

    public void testVerifyRoleQuery() throws Exception {
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        DLSRoleQueryValidator.verifyRoleQuery(queryBuilder1);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_id", "_path"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder2));
        assertThat(e.getMessage(), equalTo("terms query with terms lookup isn't supported as part of a role query"));

        QueryBuilder queryBuilder3 = new GeoShapeQueryBuilder("field", "_id");
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder3));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't supported as part of a role query"));

        QueryBuilder queryBuilder4 = new HasChildQueryBuilder("_type", new MatchAllQueryBuilder(), ScoreMode.None);
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder4));
        assertThat(e.getMessage(), equalTo("has_child query isn't supported as part of a role query"));

        QueryBuilder queryBuilder5 = new HasParentQueryBuilder("_type", new MatchAllQueryBuilder(), false);
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder5));
        assertThat(e.getMessage(), equalTo("has_parent query isn't supported as part of a role query"));

        QueryBuilder queryBuilder6 = new BoolQueryBuilder().must(new GeoShapeQueryBuilder("field", "_id"));
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder6));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't supported as part of a role query"));

        QueryBuilder queryBuilder7 = new ConstantScoreQueryBuilder(new GeoShapeQueryBuilder("field", "_id"));
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder7));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't supported as part of a role query"));

        QueryBuilder queryBuilder8 = new FunctionScoreQueryBuilder(new GeoShapeQueryBuilder("field", "_id"));
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder8));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't supported as part of a role query"));

        QueryBuilder queryBuilder9 = new BoostingQueryBuilder(new GeoShapeQueryBuilder("field", "_id"), new MatchAllQueryBuilder());
        e = expectThrows(IllegalArgumentException.class, () -> DLSRoleQueryValidator.verifyRoleQuery(queryBuilder9));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't supported as part of a role query"));
    }

    public void testHasStoredScript() throws IOException {
        assertThat(
            DLSRoleQueryValidator.hasStoredScript(new BytesArray("{\"template\":{\"id\":\"my-script\"}}"), NamedXContentRegistry.EMPTY),
            is(true)
        );
        assertThat(
            DLSRoleQueryValidator.hasStoredScript(new BytesArray("{\"template\":{\"source\":\"{}\"}}"), NamedXContentRegistry.EMPTY),
            is(false)
        );
    }

}
