/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.lucene.FilterIndexCommit;

import java.util.concurrent.atomic.AtomicBoolean;

public class SoftDeleteIndexCommit extends FilterIndexCommit {

    private final AtomicBoolean softDelete = new AtomicBoolean();

    private SoftDeleteIndexCommit(IndexCommit in) {
        super(in);
    }

    public boolean isSoftDeleted() {
        return softDelete.get();
    }

    @Override
    public void delete() {
        softDelete.compareAndSet(false, true);
        // Suppress any deletion executed by a wrapped index deletion policy.
        // We do not call super.delete() here to avoid deleting the commit immediately,
        // the commit is deleted once all references to it are released.
    }

    @Override
    public String toString() {
        return "SoftDeleteIndexCommit[" + in.getGeneration() + (isSoftDeleted() ? "](soft deleted)" : "]");
    }

    public static SoftDeleteIndexCommit wrap(IndexCommit commit) {
        assert commit instanceof SoftDeleteIndexCommit == false : commit.getClass().getName();
        return new SoftDeleteIndexCommit(commit);
    }

    public static IndexCommit unwrap(IndexCommit commit) {
        if (commit instanceof SoftDeleteIndexCommit softDeleteIndexCommit) {
            return softDeleteIndexCommit.getIndexCommit();
        }
        var error = "[" + commit.getClass().getName() + "] is not an instance of [" + SoftDeleteIndexCommit.class.getName() + ']';
        assert false : error;
        throw new IllegalStateException(error);
    }
}
