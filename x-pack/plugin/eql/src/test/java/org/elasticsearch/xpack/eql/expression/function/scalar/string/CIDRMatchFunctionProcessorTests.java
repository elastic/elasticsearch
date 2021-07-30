/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.ArrayList;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class CIDRMatchFunctionProcessorTests extends ESTestCase {

    public void testCIDRMatchFunctionValidInput() {
        // Expects null if source was null
        assertNull(new CIDRMatch(EMPTY, l(null), null).makePipe().asProcessor().process(null));

        ArrayList<Expression> addresses = new ArrayList<>();
        assertNull(new CIDRMatch(EMPTY, l(null), addresses).makePipe().asProcessor().process(null));

        assertFalse((Boolean) new CIDRMatch(EMPTY, l("10.6.48.157"), addresses).makePipe().asProcessor().process(null));

        addresses.add(l("10.6.48.157/8"));
        assertTrue((Boolean) new CIDRMatch(EMPTY, l("10.6.48.157"), addresses).makePipe().asProcessor().process(null));
    }

    public void testCIDRMatchFunctionInvalidInput() {
        ArrayList<Expression> addresses = new ArrayList<>();

        // Invalid source address
        EqlIllegalArgumentException e = expectThrows(EqlIllegalArgumentException.class,
                () -> new CIDRMatch(EMPTY, l("10.6.48"), addresses).makePipe().asProcessor().process(null));

        assertEquals("'10.6.48' is not an IP string literal.", e.getMessage());

        // Invalid match ip address
        addresses.add(l("10.6.48"));
        e = expectThrows(EqlIllegalArgumentException.class,
                () -> new CIDRMatch(EMPTY, l("10.6.48.157"), addresses).makePipe().asProcessor().process(null));

        assertEquals("'10.6.48' is not an IP string literal.", e.getMessage());
        addresses.clear();

        // Invalid CIDR
        addresses.add(l("10.6.12/12"));
        e = expectThrows(EqlIllegalArgumentException.class,
                () -> new CIDRMatch(EMPTY, l("10.6.48.157"), addresses).makePipe().asProcessor().process(null));

        assertEquals("'10.6.12' is not an IP string literal.", e.getMessage());
        addresses.clear();

        // Invalid source type
        QlIllegalArgumentException eqe = expectThrows(QlIllegalArgumentException.class,
                () -> new CIDRMatch(EMPTY, l(12345), addresses).makePipe().asProcessor().process(null));

        assertEquals("A string/char is required; received [12345]", eqe.getMessage());


        // Invalid cidr type
        addresses.add(l(5678));
        eqe = expectThrows(QlIllegalArgumentException.class,
                () -> new CIDRMatch(EMPTY, l("10.6.48.157"), addresses).makePipe().asProcessor().process(null));

        assertEquals("A string/char is required; received [5678]", eqe.getMessage());
    }
}
