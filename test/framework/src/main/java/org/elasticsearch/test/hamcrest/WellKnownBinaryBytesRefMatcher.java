/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.hamcrest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** A wrapper for matching geometries encoded as WKB in a BytesRef. */
public class WellKnownBinaryBytesRefMatcher<G extends Geometry> extends TypeSafeMatcher<BytesRef> {
    private final Matcher<G> matcher;

    public WellKnownBinaryBytesRefMatcher(Matcher<G> matcher) {
        this.matcher = matcher;
    }

    public static <G extends Geometry> Matcher<BytesRef> encodes(TypeSafeMatcher<G> matcher) {
        return new WellKnownBinaryBytesRefMatcher<G>(matcher);
    }

    @Override
    public boolean matchesSafely(BytesRef bytesRef) {
        return matcher.matches(fromBytesRef(bytesRef));
    }

    @Override
    public void describeMismatchSafely(BytesRef bytesRef, Description description) {
        matcher.describeMismatch(fromBytesRef(bytesRef), description);
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
