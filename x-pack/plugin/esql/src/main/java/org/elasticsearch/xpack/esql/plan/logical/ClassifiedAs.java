/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.RelationClass;

/**
 * Mark relations that can say what kind of relation they are, which is what the
 * {@link MetadataAttribute#RELATION_CLASS} column reports.
 * <p>
 * A relation kind binds to exactly one value, so implementors pick a nested interface rather than
 * answering for themselves: {@code implements ClassifiedAs.Dataset} is the whole declaration, and
 * there is no way for a relation to report a kind it is not.
 */
public interface ClassifiedAs {

    RelationClass relationClass();

    /** An Elasticsearch index. */
    interface Index extends ClassifiedAs {
        @Override
        default RelationClass relationClass() {
            return RelationClass.INDEX;
        }
    }

    /** An external dataset read through a format reader. */
    interface Dataset extends ClassifiedAs {
        @Override
        default RelationClass relationClass() {
            return RelationClass.DATASET;
        }
    }
}
