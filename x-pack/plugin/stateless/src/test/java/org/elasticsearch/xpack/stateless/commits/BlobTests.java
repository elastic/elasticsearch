/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.hamcrest.Matchers.equalTo;

/// Checks the contract shared by all the implementations of [Blob]: [Blob#readFrom(org.elasticsearch.common.io.stream.StreamInput, Class)]
/// reads the blob name first, and finds the implementation type from its prefix, so every implementation must write its name first.
public class BlobTests extends ESTestCase {

    /// A random blob for each implementation of [Blob]. [#testEveryImplementationIsTested()] fails if a new implementation is not added
    /// here.
    private static final Map<Class<? extends Blob>, Supplier<Blob>> RANDOM_BLOBS = Map.of(
        BccBlob.class,
        BlobTests::randomBccBlob,
        MergeBlob.class,
        BlobTests::randomMergeBlob
    );

    public void testEveryImplementationIsTested() {
        assertThat(Set.of(Blob.class.getPermittedSubclasses()), equalTo(Set.<Class<?>>copyOf(RANDOM_BLOBS.keySet())));
    }

    public void testBlobNameIsWrittenFirst() throws IOException {
        for (var randomBlob : RANDOM_BLOBS.entrySet()) {
            final Blob blob = randomBlob.getValue().get();
            try (var out = new BytesStreamOutput()) {
                blob.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    assertThat(randomBlob.getKey().getSimpleName(), in.readString(), equalTo(blob.blobName()));
                }
            }
        }
    }

    /// Reading a blob without knowing its type finds the implementation type from the blob name alone, and consumes the whole blob.
    public void testReadFindsImplementationFromBlobName() throws IOException {
        for (var randomBlob : RANDOM_BLOBS.entrySet()) {
            final Blob blob = randomBlob.getValue().get();
            try (var out = new BytesStreamOutput()) {
                blob.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    final Blob read = Blob.readFrom(in, Blob.class);
                    assertThat(read, equalTo(blob));
                    assertThat(read.getClass(), equalTo(randomBlob.getKey()));
                    assertThat(in.available(), equalTo(0));
                }
            }
        }
    }

    private static BccBlob randomBccBlob() {
        final long generation = randomNonZeroPositiveLong();
        return new BccBlob(
            BatchedCompoundCommit.blobNameFromGeneration(generation),
            new PrimaryTermAndGeneration(randomNonNegativeLong(), generation)
        );
    }

    private static MergeBlob randomMergeBlob() {
        return new MergeBlob(MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH)), randomNonNegativeLong());
    }
}
