/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils;
import org.elasticsearch.xpack.stateless.engine.HollowCommitMetadata;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;

public class HollowShardRecoveryStateSerializationTests extends AbstractWireSerializingTestCase<HollowShardRecoveryState> {

    @Override
    protected Writeable.Reader<HollowShardRecoveryState> instanceReader() {
        return HollowShardRecoveryState::new;
    }

    @Override
    protected HollowShardRecoveryState createTestInstance() {
        final var termAndGeneration = new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong());
        final var commit = randomCompoundCommit(randomShardId(), termAndGeneration, true);
        final var metadata = new HollowCommitMetadata(
            termAndGeneration.generation(),
            randomHollowUserData(),
            randomByteArrayOfLength(StringHelper.ID_LENGTH),
            randomLuceneVersion(),
            randomIntBetween(0, 1000),
            randomIntBetween(1000, 2000),
            randomIntBetween(0, 100),
            commit.commitFiles().keySet()
        );
        return new HollowShardRecoveryState(new BatchedCompoundCommit(termAndGeneration, List.of(commit)), metadata);
    }

    @Override
    protected HollowShardRecoveryState mutateInstance(HollowShardRecoveryState instance) {
        final var metadata = instance.metadata();
        final int mutation = between(0, 6);
        if (mutation == 6) {
            // the metadata must describe the hollow commit, so a new generation comes with a new commit and its files
            final var termAndGeneration = new PrimaryTermAndGeneration(
                instance.commit().primaryTermAndGeneration().primaryTerm(),
                randomValueOtherThan(metadata.generation(), StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong)
            );
            final var commit = randomCompoundCommit(instance.commit().lastCompoundCommit().shardId(), termAndGeneration, true);
            return new HollowShardRecoveryState(
                new BatchedCompoundCommit(termAndGeneration, List.of(commit)),
                new HollowCommitMetadata(
                    termAndGeneration.generation(),
                    metadata.userData(),
                    metadata.id(),
                    metadata.luceneVersion(),
                    metadata.numDocs(),
                    metadata.maxDoc(),
                    metadata.segmentCount(),
                    commit.commitFiles().keySet()
                )
            );
        }
        final var mutated = switch (mutation) {
            case 0 -> {
                final var userData = new HashMap<>(metadata.userData());
                userData.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
                yield copy(metadata, userData, metadata.id(), metadata.luceneVersion(), metadata.numDocs(), metadata.maxDoc());
            }
            case 1 -> copy(
                metadata,
                metadata.userData(),
                randomArrayOtherThan(metadata.id(), () -> randomByteArrayOfLength(StringHelper.ID_LENGTH)),
                metadata.luceneVersion(),
                metadata.numDocs(),
                metadata.maxDoc()
            );
            case 2 -> copy(
                metadata,
                metadata.userData(),
                metadata.id(),
                randomValueOtherThan(metadata.luceneVersion(), HollowShardRecoveryStateSerializationTests::randomLuceneVersion),
                metadata.numDocs(),
                metadata.maxDoc()
            );
            case 3 -> copy(
                metadata,
                metadata.userData(),
                metadata.id(),
                metadata.luceneVersion(),
                metadata.numDocs() + 1,
                metadata.maxDoc()
            );
            case 4 -> copy(
                metadata,
                metadata.userData(),
                metadata.id(),
                metadata.luceneVersion(),
                metadata.numDocs(),
                metadata.maxDoc() + 1
            );
            case 5 -> new HollowCommitMetadata(
                metadata.generation(),
                metadata.userData(),
                metadata.id(),
                metadata.luceneVersion(),
                metadata.numDocs(),
                metadata.maxDoc(),
                metadata.segmentCount() + 1,
                metadata.files()
            );
            default -> throw new AssertionError("unexpected mutation");
        };
        return new HollowShardRecoveryState(instance.commit(), mutated);
    }

    private static HollowCommitMetadata copy(
        HollowCommitMetadata metadata,
        Map<String, String> userData,
        byte[] id,
        Version luceneVersion,
        int numDocs,
        int maxDoc
    ) {
        return new HollowCommitMetadata(
            metadata.generation(),
            userData,
            id,
            luceneVersion,
            numDocs,
            maxDoc,
            metadata.segmentCount(),
            metadata.files()
        );
    }

    private static Map<String, String> randomHollowUserData() {
        final Map<String, String> userData = new HashMap<>(randomMap(0, 5, () -> tuple(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        userData.put(
            StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE,
            Long.toString(StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE)
        );
        return userData;
    }

    private static Version randomLuceneVersion() {
        if (randomBoolean()) {
            return Version.fromBits(between(1, Version.LATEST.major), between(0, 20), between(0, 10));
        }
        try {
            // a prerelease version, which only round-trips through its string representation
            return Version.parse(between(1, Version.LATEST.major) + ".0.0." + between(1, 2));
        } catch (ParseException e) {
            throw new AssertionError(e);
        }
    }
}
