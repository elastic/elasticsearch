/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractScriptFieldQueryTestCase<T extends AbstractScriptFieldQuery> extends ESTestCase {
    protected abstract T createTestInstance();

    protected abstract T copy(T orig);

    protected abstract T mutate(T orig);

    protected final Script randomScript() {
        return new Script(randomAlphaOfLength(10));
    }

    public final void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::mutate);
    }

    public abstract void testMatches();

    public final void testToString() {
        T query = createTestInstance();
        assertThat(query.toString(), equalTo(query.fieldName() + ":" + query.toString(query.fieldName())));
        assertToString(query);
    }

    protected abstract void assertToString(T query);

    public abstract void testVisit();
}
