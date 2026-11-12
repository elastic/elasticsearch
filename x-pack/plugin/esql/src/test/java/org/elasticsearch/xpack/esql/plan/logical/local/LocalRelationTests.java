/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/** {@link LocalRelation#readableRepeatedly()}, asked of the relation because three callers share it. */
public class LocalRelationTests extends ESTestCase {

    private static final List<Attribute> OUTPUT = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));

    public void testARelationCarryingAPageIsMadeToCopyPerRead() {
        LocalRelation shared = relation(onePage()).readableRepeatedly();

        assertThat(shared.supplier(), instanceOf(CopyingLocalSupplier.class));
    }

    public void testARelationThatAlreadyCopiesIsReturnedUnchanged() {
        LocalRelation copying = relation(onePage()).readableRepeatedly();

        assertSame(copying, copying.readableRepeatedly());
    }

    /** Losing emptiness costs a prune and not an answer, so nothing asserting on results catches it. */
    public void testAnEmptyRelationStillReadsAsEmptyAfterSharing() {
        LocalRelation empty = relation(EmptyLocalSupplier.EMPTY);

        LocalRelation shared = empty.readableRepeatedly();

        assertSame(empty, shared);
        assertTrue("shared by several parents and no longer recognisable as empty", shared.hasEmptySupplier());
    }

    private static LocalRelation relation(LocalSupplier supplier) {
        return new LocalRelation(Source.EMPTY, OUTPUT, supplier);
    }

    private static LocalSupplier onePage() {
        return LocalSupplier.of(new Page(BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 1, 1)));
    }
}
