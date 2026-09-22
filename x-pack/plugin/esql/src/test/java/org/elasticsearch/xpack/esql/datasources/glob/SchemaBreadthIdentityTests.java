/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.InferredFrom;
import org.elasticsearch.xpack.esql.datasources.SchemaBreadth;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/**
 * How each {@link SchemaBreadth} identifies the files a cached schema was inferred from. Lives beside the listing
 * representations because that is what it has to be right about: the resolver usually holds a compacted listing, and
 * sometimes a truncated one, and an identity that changed with the representation would miss where it must hit.
 */
public class SchemaBreadthIdentityTests extends ESTestCase {

    private static final String BASE = "s3://bucket/data/";

    private static StorageEntry entry(String name, long size, long mtimeMillis) {
        return new StorageEntry(StoragePath.of(BASE + name), size, Instant.ofEpochMilli(mtimeMillis));
    }

    private static FileList listing(StorageEntry... entries) {
        return GlobExpander.fileListOf(List.of(entries), BASE + "*.parquet");
    }

    private static final StorageEntry A = entry("a.parquet", 100, 5_000);
    private static final StorageEntry B = entry("b.parquet", 200, 5_001);
    private static final StorageEntry C = entry("c.parquet", 300, 5_002);
    private static final StorageEntry Z = entry("z.parquet", 400, 5_003);
    private static final StorageEntry EARLIER = entry("0.parquet", 50, 4_999);

    public void testOneFileSurvivesAnAppendAfterTheAnchor() {
        assertEquals(
            "a file appended after the anchor must not change what first_file_wins inferred from",
            SchemaBreadth.ONE_FILE.inferredFrom(listing(A, B, C)),
            SchemaBreadth.ONE_FILE.inferredFrom(listing(A, B, C, Z))
        );
    }

    public void testOneFileChangesWhenAFileArrivesBeforeTheAnchor() {
        assertNotEquals(
            "a file arriving before the anchor becomes the anchor",
            SchemaBreadth.ONE_FILE.inferredFrom(listing(A, B, C)),
            SchemaBreadth.ONE_FILE.inferredFrom(listing(EARLIER, A, B, C))
        );
    }

    public void testOneFileChangesWhenTheAnchorItselfChanges() {
        StorageEntry rewrittenAnchor = entry("a.parquet", 100, 9_999);
        assertNotEquals(
            SchemaBreadth.ONE_FILE.inferredFrom(listing(A, B, C)),
            SchemaBreadth.ONE_FILE.inferredFrom(listing(rewrittenAnchor, B, C))
        );
    }

    public void testEveryFileIsOrderIndependentButChangesOnAnyMembershipChange() {
        InferredFrom abc = SchemaBreadth.EVERY_FILE.inferredFrom(listing(A, B, C));
        assertEquals("the same set in another order", abc, SchemaBreadth.EVERY_FILE.inferredFrom(listing(C, A, B)));
        assertNotEquals("an appended file", abc, SchemaBreadth.EVERY_FILE.inferredFrom(listing(A, B, C, Z)));
        assertNotEquals("a removed file", abc, SchemaBreadth.EVERY_FILE.inferredFrom(listing(A, B)));
    }

    public void testTheTwoBreadthsNeverShareAnIdentity() {
        FileList abc = listing(A, B, C);
        InferredFrom oneFile = SchemaBreadth.ONE_FILE.inferredFrom(abc);
        InferredFrom everyFile = SchemaBreadth.EVERY_FILE.inferredFrom(abc);
        assertThat(oneFile, instanceOf(InferredFrom.Anchor.class));
        assertThat(everyFile, instanceOf(InferredFrom.EveryFile.class));
        assertNotEquals(oneFile, everyFile);
    }

    public void testDeclarationCachesNothing() {
        assertNull(SchemaBreadth.DECLARATION.inferredFrom(listing(A, B, C)));
    }

    public void testFewerThanTwoFilesAndSentinelsKeyNothing() {
        for (SchemaBreadth breadth : SchemaBreadth.values()) {
            assertNull(breadth + " over one file", breadth.inferredFrom(listing(A)));
            assertNull(breadth + " over EMPTY", breadth.inferredFrom(FileList.EMPTY));
            assertNull(breadth + " over UNRESOLVED", breadth.inferredFrom(FileList.UNRESOLVED));
        }
    }

    /**
     * A truncated listing is a prefix of the files the pattern matches. It still has a head, so first_file_wins can be
     * keyed on it; it does not have the whole set, so union_by_name and strict cannot.
     */
    public void testATruncatedListingKeysTheAnchorButNotTheSet() {
        FileList truncated = new GenericFileList(List.of(A, B, C), BASE + "*.parquet", null, List.of(), true);
        assertTrue(truncated.isTruncated());
        assertEquals(SchemaBreadth.ONE_FILE.inferredFrom(listing(A, B, C)), SchemaBreadth.ONE_FILE.inferredFrom(truncated));
        assertNull(SchemaBreadth.EVERY_FILE.inferredFrom(truncated));
    }

    /**
     * The representation the listing cache holds must not change the identity: compacting a listing must leave both
     * breadths' identities exactly as they were.
     */
    public void testCompactionLeavesEveryIdentityUnchanged() {
        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(entry("part-" + i + ".parquet", 1_000 + i, 5_000 + i));
        }
        FileList raw = GlobExpander.fileListOf(entries, BASE + "*.parquet");
        FileList compact = GlobExpander.compact(raw, BASE);
        assertNotSame("the listing must actually compact", raw, compact);
        for (SchemaBreadth breadth : SchemaBreadth.values()) {
            assertEquals(breadth + " across compaction", breadth.inferredFrom(raw), breadth.inferredFrom(compact));
        }
    }
}
