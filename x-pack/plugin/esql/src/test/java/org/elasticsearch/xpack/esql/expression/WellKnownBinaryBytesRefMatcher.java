/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class WellKnownBinaryBytesRefMatcher<G extends Geometry> extends BaseMatcher<BytesRef> {
    private final Matcher<G> matcher;

    public WellKnownBinaryBytesRefMatcher(Matcher<G> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matches(Object item) {
        if (item instanceof BytesRef bytesRef) {
            return matcher.matches(fromBytesRef(bytesRef));
        }
        return false;
    }

    @Override
    public void describeMismatch(Object item, Description description) {
        if (item instanceof BytesRef bytesRef) {
            matcher.describeMismatch(fromBytesRef(bytesRef), description);
        } else {
            description.appendText("was ").appendValue(item);
        }
    }

    @SuppressWarnings("unchecked")
    private G fromBytesRef(BytesRef bytesRef) {
        return (G) WellKnownBinary.fromWKB(GeometryValidator.NOOP, false /* coerce */, bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    @Override
    public void describeTo(Description description) {
        matcher.describeTo(description);
    }
}
