/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.mapper.flattened.FlattenedKeysConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FlattenedKeysTests extends ESTestCase {
    public void testKeepsTheMatchingKeys() {
        assertThat(
            process(
                "{\"host.ip\":\"1.2.3.4\",\"host.name\":\"h\",\"secret\":\"s\",\"tags\":[\"a\",\"b\"]}",
                List.of("host.*", "tags"),
                List.of("host.ip")
            ),
            equalTo("{\"host.name\":\"h\",\"tags\":[\"a\",\"b\"]}")
        );
    }

    public void testNoMatchingKeyIsNull() {
        assertThat(process("{\"a\":\"1\"}", List.of("b*"), List.of()), nullValue());
    }

    public void testTruncatedValueIsInvalid() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> process("{\"a\":\"1\"", List.of("a"), List.of()));
        assertThat(e.getMessage(), equalTo("invalid JSON input"));
    }

    public void testEqualsComparesTheKeys() {
        ReferenceAttribute attrs = new ReferenceAttribute(Source.EMPTY, null, "attrs", DataType.FLATTENED);
        FlattenedKeys b = new FlattenedKeys(Source.EMPTY, attrs, List.of("b*"), List.of());
        assertThat(b, equalTo(new FlattenedKeys(Source.EMPTY, attrs, List.of("b*"), List.of())));
        assertThat(b.hashCode(), equalTo(new FlattenedKeys(Source.EMPTY, attrs, List.of("b*"), List.of()).hashCode()));
        assertThat(b, not(equalTo(new FlattenedKeys(Source.EMPTY, attrs, List.of("c*"), List.of()))));
        assertThat(b, not(equalTo(new FlattenedKeys(Source.EMPTY, attrs, List.of(), List.of("b*")))));
    }

    public void testPushesToTheLoaderOfAField() {
        FieldAttribute attrs = new FieldAttribute(
            Source.EMPTY,
            "attrs",
            new EsField("attrs", DataType.FLATTENED, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        PushedBlockLoaderExpression pushed = new FlattenedKeys(Source.EMPTY, attrs, List.of("b*"), List.of("bar")).tryPushToFieldLoading(
            SearchStats.EMPTY
        );
        assertThat(pushed.field(), equalTo(attrs));
        assertThat(pushed.config(), equalTo(new FlattenedKeysConfig(List.of("b*"), List.of("bar"))));
    }

    public void testDoesNotPushOverAComputedValue() {
        ReferenceAttribute attrs = new ReferenceAttribute(Source.EMPTY, null, "attrs", DataType.FLATTENED);
        assertThat(new FlattenedKeys(Source.EMPTY, attrs, List.of("b*"), List.of()).tryPushToFieldLoading(SearchStats.EMPTY), nullValue());
    }

    private static String process(String flattened, List<String> includes, List<String> excludes) {
        try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(1)) {
            FlattenedKeys.process(builder, new BytesRef(flattened), new FlattenedKeysConfig(includes, excludes));
            try (BytesRefBlock block = builder.build()) {
                return block.isNull(0) ? null : block.getBytesRef(0, new BytesRef()).utf8ToString();
            }
        }
    }
}
