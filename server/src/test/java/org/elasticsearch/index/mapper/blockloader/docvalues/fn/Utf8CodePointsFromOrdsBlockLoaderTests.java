/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class Utf8CodePointsFromOrdsBlockLoaderTests extends AbstractFromOrdsBlockLoaderTests {
    public Utf8CodePointsFromOrdsBlockLoaderTests(boolean highCardinality, boolean multiValues, boolean missingValues) {
        super(highCardinality, multiValues, missingValues);
    }

    @Override
    protected void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException {
        List<MockWarnings.MockWarning> expectedWarnings = new ArrayList<>();
        for (int i = 0; i < mvCount; i++) {
            expectedWarnings.add(
                new MockWarnings.MockWarning(IllegalArgumentException.class, "single-value function encountered multi-value")
            );
        }

        var warnings = new MockWarnings();
        var stringsLoader = new BytesRefsFromOrdsBlockLoader("field", ByteSizeValue.ofKb(randomLongBetween(1, 1000)));
        var codePointsLoader = new Utf8CodePointsFromOrdsBlockLoader(warnings, "field", ByteSizeValue.ofKb(randomLongBetween(1, 1000)));
        BlockLoader.Docs docs = TestBlock.docs(ctx);

        try (var stringsReader = stringsLoader.reader(breaker, ctx); var codePointsReader = codePointsLoader.reader(breaker, ctx)) {
            assertThat(codePointsReader, readerMatcher());
            try (TestBlock strings = read(stringsReader, docs); TestBlock codePoints = read(codePointsReader, docs);) {
                checkBlocks(strings, codePoints);
            }
            assertThat(warnings.warnings(), equalTo(expectedWarnings));
            warnings.warnings().clear();
        }

        try (var stringsReader = stringsLoader.reader(breaker, ctx); var codePointsReader = codePointsLoader.reader(breaker, ctx);) {
            for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                for (int d = 0; d < docsArray.length; d++) {
                    docsArray[d] = i + d;
                }
                docs = TestBlock.docs(docsArray);
                try (TestBlock strings = read(stringsReader, docs); TestBlock codePoints = read(codePointsReader, docs);) {
                    checkBlocks(strings, codePoints);
                }
            }
            assertThat(warnings.warnings(), equalTo(expectedWarnings));
        }
    }

    private Matcher<Object> readerMatcher() {
        if (lowCardinality == false) {
            return hasToString("Utf8CodePointsFromOrds.Immediate");
        }
        if (multiValues) {
            return hasToString("Utf8CodePointsFromOrds.SortedSet");
        }
        return hasToString("Utf8CodePointsFromOrds.Singleton");
    }

    private static KeywordFieldMapper.KeywordField field(int codePointCount) {
        return new KeywordFieldMapper.KeywordField(
            "field",
            new BytesRef("a".repeat(codePointCount)),
            KeywordFieldMapper.Defaults.FIELD_TYPE
        );
    }

    private void checkBlocks(TestBlock strings, TestBlock codePoints) {
        for (int i = 0; i < strings.size(); i++) {
            Object str = strings.get(i);
            if (str instanceof List<?> || str == null) {
                assertThat(codePoints.get(i), nullValue());
                continue;
            }
            BytesRef bytes = (BytesRef) str;
            assertThat(codePoints.get(i), equalTo(bytes.length));
        }
    }
}
