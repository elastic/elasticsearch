/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Set;

/**
 * The indices of a {@link TextEsField} that report the same index analyzer. Kept only when the queried indices
 * disagree, so HIGHLIGHT can pick the analyzer of the index each row came from.
 *
 * @param analyzerName          the reported analyzer, or {@code null} when it was withheld or not reported at all
 * @param indexLocal            whether a {@code null} name was withheld because the analyzer is an {@code index.analysis}
 *                              definition, rather than not reported by an older node
 * @param positionIncrementGap  the field's {@code position_increment_gap} under that analyzer
 * @param indices               concrete index names, cluster-qualified for remote indices like {@code _index} values
 */
public record IndexAnalyzerGroup(@Nullable String analyzerName, boolean indexLocal, int positionIncrementGap, Set<String> indices)
    implements
        Writeable {

    public IndexAnalyzerGroup {
        assert analyzerName == null || indexLocal == false : "a reported analyzer name cannot be index-local";
        indices = Set.copyOf(indices);
    }

    public IndexAnalyzerGroup(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readBoolean(), in.readVInt(), in.readCollectionAsImmutableSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(analyzerName);
        out.writeBoolean(indexLocal);
        out.writeVInt(positionIncrementGap);
        out.writeStringCollection(indices);
    }
}
