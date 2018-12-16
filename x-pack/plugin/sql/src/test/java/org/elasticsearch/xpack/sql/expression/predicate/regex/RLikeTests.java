package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RLikeTests extends ESTestCase {

    private static final Location EMPTY = new Location(-1, -2);

    public void testNullArguments() {

        boolean exceptionCaught = false;

        try
        {
           new RLike(null, null, null);
        }
        catch (SqlIllegalArgumentException exception)
        {
            exceptionCaught = true;
        }

        assertEquals(exceptionCaught, true);

    }

    public void testNullChildren() {

        boolean exceptionCaught = false;

        RLike rlike = new RLike(EMPTY, Literal.of(EMPTY, 1), Literal.of(EMPTY, 1));

        try
        {
            rlike.replaceChildren(null, Literal.of(EMPTY, 1));
        }
        catch (SqlIllegalArgumentException exception)
        {
            exceptionCaught = true;
        }

        assertEquals(exceptionCaught, true);

    }

    public void testSimpleConstructor() {

        int n1 = randomInteger(), n2 = randomInteger();

        RLike rlike = new RLike(EMPTY, Literal.of(EMPTY, n1), Literal.of(EMPTY, n2));

        List<Expression> children = rlike.children();

        assertEquals(children.size(), 2);

        assertEquals(children.get(0).toString(), Integer.toString(n1));
        assertEquals(children.get(1).toString(), Integer.toString(n2));
    }

    public void testSimpleChildren() {

        int n1 = randomInteger(), n2 = randomInteger(), n3 = randomInteger(), n4 = randomInteger();

        RLike rlike = new RLike(EMPTY, Literal.of(EMPTY, n1), Literal.of(EMPTY, n2));

        rlike = rlike.replaceChildren(Literal.of(EMPTY, n3), Literal.of(EMPTY, n4));

        List<Expression> children = rlike.children();
        assertEquals(children.size(), 2);

        assertEquals(children.get(0).toString(), Integer.toString(n3));
        assertEquals(children.get(1).toString(), Integer.toString(n4));
    }

    public void testToString() {

        int n1 = randomInteger(), n2 = randomInteger();

        RLike rlike = new RLike(EMPTY , Literal.of(EMPTY, n1), Literal.of(EMPTY, n2));

        String testString = rlike.toString().substring(0, rlike.toString().indexOf('#'));

        assertEquals(testString, n1 + " REGEX " + n2);
    }


    private static int randomInteger()
    {
        return ThreadLocalRandom.current().nextInt(-10, 11);
    }
}

