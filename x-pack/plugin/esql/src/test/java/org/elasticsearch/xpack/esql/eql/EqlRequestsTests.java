/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link EqlRequests#build} — mapping the {@code EQL <indexPattern> "..." WITH { ... }} command's
 * index pattern, resolved schema and tuning options to an {@link EqlSearchRequest}.
 */
public class EqlRequestsTests extends ESTestCase {

    private static final List<Attribute> NO_SCHEMA = List.of();

    public void testRequiresIndexPattern() {
        EsqlIllegalArgumentException e = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> EqlRequests.build("process where true", "  ", NO_SCHEMA, Map.of())
        );
        assertThat(e.getMessage(), containsString("non-empty index pattern"));
    }

    public void testSingleIndexAndQuery() {
        EqlSearchRequest request = EqlRequests.build("process where true", "logs-*", NO_SCHEMA, Map.of());
        assertThat(request.indices(), arrayContaining("logs-*"));
        assertThat(request.query(), equalTo("process where true"));
    }

    public void testCommaSeparatedIndicesAreSplitAndTrimmed() {
        EqlSearchRequest request = EqlRequests.build("process where true", "logs-a, logs-b ,logs-c", NO_SCHEMA, Map.of());
        assertThat(request.indices(), arrayContaining("logs-a", "logs-b", "logs-c"));
    }

    public void testFetchFieldsFromSchema() {
        List<Attribute> schema = List.of(
            new ReferenceAttribute(EMPTY, "_sequence", LONG), // synthetic — no wire field
            fieldAttribute("process.name", KEYWORD),
            fieldAttribute("@timestamp", DATETIME),
            new UnsupportedAttribute(EMPTY, "blob", new UnsupportedEsField("blob", List.of("binary"), null, Map.of())) // excluded
        );
        EqlSearchRequest request = EqlRequests.build("process where true", "logs", schema, Map.of());
        List<FieldAndFormat> fields = request.fetchFields();
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).field, equalTo("process.name"));
        assertThat(fields.get(0).format, nullValue());
        assertThat(fields.get(1).field, equalTo("@timestamp"));
        assertThat(fields.get(1).format, equalTo("epoch_millis"));
    }

    public void testNoFetchFieldsWhenSchemaHasNoMappedFields() {
        List<Attribute> schema = List.of(new ReferenceAttribute(EMPTY, "_sequence", LONG));
        EqlSearchRequest request = EqlRequests.build("process where true", "logs", schema, Map.of());
        assertThat(request.fetchFields(), nullValue());
    }

    public void testOptionalTuning() {
        EqlSearchRequest request = EqlRequests.build(
            "process where true",
            "logs",
            NO_SCHEMA,
            Map.of(
                "size",
                42,
                "fetch_size",
                500,
                "timestamp_field",
                "ts",
                "tiebreaker_field",
                "seq",
                "event_category_field",
                "cat",
                "result_position",
                "head"
            )
        );
        assertThat(request.size(), equalTo(42));
        assertThat(request.fetchSize(), equalTo(500));
        assertThat(request.timestampField(), equalTo("ts"));
        assertThat(request.tiebreakerField(), equalTo("seq"));
        assertThat(request.eventCategoryField(), equalTo("cat"));
        assertThat(request.resultPosition(), equalTo("head"));
    }
}
