/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

public class DocRefEncodeExecSerializationTests extends AbstractPhysicalPlanSerializationTests<DocRefEncodeExec> {
    public static DocRefEncodeExec randomDocRefEncodeExec(int depth) {
        return new DocRefEncodeExec(randomSource(), randomChild(depth), randomDoc(), randomDocRef());
    }

    static Attribute randomDoc() {
        return new FieldAttribute(randomSource(), null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
    }

    static ReferenceAttribute randomDocRef() {
        return new ReferenceAttribute(randomSource(), null, randomAlphaOfLength(5), DataType.DOC_REF, Nullability.FALSE, null, true);
    }

    @Override
    protected DocRefEncodeExec createTestInstance() {
        return randomDocRefEncodeExec(0);
    }

    @Override
    protected DocRefEncodeExec mutateInstance(DocRefEncodeExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute doc = instance.doc();
        ReferenceAttribute docRef = instance.docRef();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> doc = randomValueOtherThan(doc, DocRefEncodeExecSerializationTests::randomDoc);
            case 2 -> docRef = randomValueOtherThan(docRef, DocRefEncodeExecSerializationTests::randomDocRef);
            default -> throw new AssertionError("Unexpected case");
        }
        return new DocRefEncodeExec(instance.source(), child, doc, docRef);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
