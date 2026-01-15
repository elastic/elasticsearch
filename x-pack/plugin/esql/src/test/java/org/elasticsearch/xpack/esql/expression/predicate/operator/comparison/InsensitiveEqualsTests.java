package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

// Trigger PR update
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class InsensitiveEqualsTests extends AbstractScalarFunctionTestCase {

    public InsensitiveEqualsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.stringCases(
                (l, r) -> l.toString().equalsIgnoreCase(r.toString()),
                (lhsType, rhsType) -> "InsensitiveEqualsKeywordsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                List.of(),
                DataType.BOOLEAN
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new InsensitiveEquals(source, args.get(0), args.get(1));
    }

    public void testFoldSpecific() {
        assertTrue(insensitiveEquals(l("foo"), l("foo")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("Foo"), l("foo")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("Foo"), l("fOO")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo*"), l("foo*")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo*"), l("FOO*")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo?bar"), l("foo?bar")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo?bar"), l("FOO?BAR")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l(""), l("")).fold(FoldContext.small()));

        assertFalse(insensitiveEquals(l("Foo"), l("fo*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("Fox"), l("fo?")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("Foo"), l("*OO")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("BarFooBaz"), l("*O*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("BarFooBaz"), l("bar*baz")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("*")).fold(FoldContext.small()));

        assertFalse(insensitiveEquals(l("foo*bar"), l("foo\\*bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo?"), l("foo\\?")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo?bar"), l("foo\\?bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(10)), l("*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("???")).fold(FoldContext.small()));

        assertFalse(insensitiveEquals(l("foo"), l("bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("ba*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("*a*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(""), l("bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("??")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("????")).fold(FoldContext.small()));

        assertNull(insensitiveEquals(l("foo"), Literal.NULL).fold(FoldContext.small()));
        assertNull(insensitiveEquals(Literal.NULL, l("foo")).fold(FoldContext.small()));
        assertNull(insensitiveEquals(Literal.NULL, Literal.NULL).fold(FoldContext.small()));
    }

    public void testProcess() {
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("foo")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("foo")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fOO")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("foo*")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("FOO*")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("foo?bar")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("FOO?BAR")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef(""), BytesRefs.toBytesRef("")));

        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fo*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("Fox"), BytesRefs.toBytesRef("fo?")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("*OO")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("BarFooBaz"), BytesRefs.toBytesRef("*O*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("BarFooBaz"), BytesRefs.toBytesRef("bar*baz")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("*")));

        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*bar"), BytesRefs.toBytesRef("foo\\*bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?"), BytesRefs.toBytesRef("foo\\?")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("foo\\?bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(10)), BytesRefs.toBytesRef("*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("???")));

        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("ba*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("*a*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(""), BytesRefs.toBytesRef("bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("??")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("????")));
    }

    protected InsensitiveEquals insensitiveEquals(Expression left, Expression right) {
        return new InsensitiveEquals(EMPTY, left, right);
    }

    private static Literal l(Object value) {
        return of(EMPTY, value);
    }
}
