package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public abstract class AbstractStringScriptFieldQueryTestCase<T extends AbstractStringScriptFieldQuery> extends ESTestCase {
    protected final StringScriptFieldScript.LeafFactory leafFactory = mock(StringScriptFieldScript.LeafFactory.class);

    protected abstract T createTestInstance();

    protected abstract T copy(T orig);

    protected abstract T mutate(T orig);

    public final void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::mutate);
    }

    public final void testToString() {
        T query = createTestInstance();
        assertThat(query.toString(), equalTo(query.fieldName() + ":" + query.toString(query.fieldName())));
    }

    protected abstract void assertToString(T query);

    public abstract void testVisit();
}
