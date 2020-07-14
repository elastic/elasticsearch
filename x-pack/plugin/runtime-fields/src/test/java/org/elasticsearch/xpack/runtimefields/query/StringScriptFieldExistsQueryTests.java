package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldExistsQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldExistsQuery> {
    @Override
    protected StringScriptFieldExistsQuery createTestInstance() {
        return new StringScriptFieldExistsQuery(leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected StringScriptFieldExistsQuery copy(StringScriptFieldExistsQuery orig) {
        return new StringScriptFieldExistsQuery(leafFactory, orig.fieldName());
    }

    @Override
    protected StringScriptFieldExistsQuery mutate(StringScriptFieldExistsQuery orig) {
        return new StringScriptFieldExistsQuery(leafFactory, orig.fieldName() + "modified");
    }

    @Override
    protected void assertToString(StringScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("*"));
    }

    @Override
    public void testVisit() {
        createTestInstance().visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                fail();
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                fail();
            }
        });
    }
}
