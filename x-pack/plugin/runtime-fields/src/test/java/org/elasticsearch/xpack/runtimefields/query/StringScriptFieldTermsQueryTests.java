package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toCollection;
import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldTermsQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldTermsQuery> {
    @Override
    protected StringScriptFieldTermsQuery createTestInstance() {
        Set<String> terms = new TreeSet<>(Arrays.asList(generateRandomStringArray(4, 6, false, false)));
        return new StringScriptFieldTermsQuery(leafFactory, randomAlphaOfLength(5), terms);
    }

    @Override
    protected StringScriptFieldTermsQuery copy(StringScriptFieldTermsQuery orig) {
        return new StringScriptFieldTermsQuery(leafFactory, orig.fieldName(), orig.terms());
    }

    @Override
    protected StringScriptFieldTermsQuery mutate(StringScriptFieldTermsQuery orig) {
        if (randomBoolean()) {
            return new StringScriptFieldTermsQuery(leafFactory, orig.fieldName() + "modified", orig.terms());
        }
        Set<String> terms = new TreeSet<>(orig.terms());
        terms.add(randomAlphaOfLength(7));
        return new StringScriptFieldTermsQuery(leafFactory, orig.fieldName(), terms);
    }

    @Override
    protected void assertToString(StringScriptFieldTermsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.terms().toString()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldTermsQuery query = createTestInstance();
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
        assertThat(allTerms, equalTo(query.terms().stream().map(t -> new Term(query.fieldName(), t)).collect(toCollection(TreeSet::new))));
    }
}
