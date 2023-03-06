/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class StartsWithTests extends AbstractScalarFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        String str = randomAlphaOfLength(5);
        String prefix = randomAlphaOfLength(5);
        if (randomBoolean()) {
            str = prefix + str;
        }
        return List.of(new BytesRef(str), new BytesRef(prefix));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new StartsWith(Source.EMPTY, field("str", DataTypes.KEYWORD), field("prefix", DataTypes.KEYWORD));
    }

    @Override
    protected DataType expressionForSimpleDataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data) {
        String str = ((BytesRef) data.get(0)).utf8ToString();
        String prefix = ((BytesRef) data.get(1)).utf8ToString();
        return equalTo(str.startsWith(prefix));
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "StartsWithEvaluator[str=Keywords[channel=0], prefix=Keywords[channel=1]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new StartsWith(
            Source.EMPTY,
            new Literal(Source.EMPTY, (BytesRef) data.get(0), DataTypes.KEYWORD),
            new Literal(Source.EMPTY, (BytesRef) data.get(1), DataTypes.KEYWORD)
        );
    }

    @Override
    public void testResolveTypeInvalid() {
        for (DataType t1 : EsqlDataTypes.types()) {
            if (t1 == DataTypes.KEYWORD || t1 == DataTypes.NULL) {
                continue;
            }
            for (DataType t2 : EsqlDataTypes.types()) {
                if (t2 == DataTypes.KEYWORD || t2 == DataTypes.NULL) {
                    continue;
                }
                Expression.TypeResolution resolution = new StartsWith(
                    new Source(Location.EMPTY, "foo"),
                    new Literal(new Source(Location.EMPTY, "str"), "str", t1),
                    new Literal(new Source(Location.EMPTY, "str"), "str", t2)
                ).resolveType();
                assertFalse("resolution for [" + t1 + "/" + t2 + "]", resolution.resolved());
                assertThat(resolution.message(), containsString("argument of [foo] must be [string], found value ["));
            }
        }
    }
}
