package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldTermQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldTermQuery> {
    @Override
    protected StringScriptFieldTermQuery createTestInstance() {
        return new StringScriptFieldTermQuery(leafFactory, randomAlphaOfLength(5), randomAlphaOfLength(6));
    }

    @Override
    protected StringScriptFieldTermQuery copy(StringScriptFieldTermQuery orig) {
        return new StringScriptFieldTermQuery(leafFactory, orig.fieldName(), orig.term());
    }

    @Override
    protected StringScriptFieldTermQuery mutate(StringScriptFieldTermQuery orig) {
        if (randomBoolean()) {
            return new StringScriptFieldTermQuery(leafFactory, orig.fieldName() + "modified", orig.term());
        }
        return new StringScriptFieldTermQuery(leafFactory, orig.fieldName(), orig.term() + "modified");
    }

    @Override
    protected void assertToString(StringScriptFieldTermQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.term()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldTermQuery query = createTestInstance(); 
        Set<Term> allTerms = new TreeSet<>();
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                allTerms.addAll(Arrays.asList(terms));
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                fail();
            }
        });
        assertThat(allTerms, equalTo(Set.of(new Term(query.fieldName(), query.term()))));
    }
}
