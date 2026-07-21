/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

/**
 * An ordered chain of {@link BlockTransform}s feeding one {@link BlockTerminal}. Transforms fire
 * adaptively per block; the terminal always runs. A pipeline is described on disk by its stage ids, so
 * a reader that knows more stages than were written still decodes old data.
 *
 * <p>Stage ids are frozen once shipped. Adding a stage is additive: define a new id, register it in
 * {@link Registry}, and append it to a pipeline; existing columns list only their old ids and are
 * unaffected.
 */
public final class NumericPipeline {

    private final BlockTransform[] transforms;
    private final BlockTerminal terminal;

    public NumericPipeline(BlockTransform[] transforms, BlockTerminal terminal) {
        this.transforms = transforms;
        this.terminal = terminal;
    }

    /** The default chain: delta, offset, GCD, then FOR bit-packing. */
    public static NumericPipeline defaultPipeline(int blockSize) {
        return new NumericPipeline(
            new BlockTransform[] { new DeltaTransform(), new OffsetTransform(), new GcdTransform() },
            new ForTerminal(blockSize)
        );
    }

    BlockTransform[] transforms() {
        return transforms;
    }

    BlockTerminal terminal() {
        return terminal;
    }

    /** The transform ids in pipeline order, recorded in the column metadata. */
    public byte[] transformIds() {
        byte[] ids = new byte[transforms.length];
        for (int i = 0; i < transforms.length; i++) {
            ids[i] = transforms[i].id();
        }
        return ids;
    }

    /** The terminal id, recorded in the column metadata. */
    public byte terminalId() {
        return terminal.id();
    }

    /**
     * Maps frozen stage ids back to stages so a reader can rebuild the exact pipeline a column was
     * written with. New stages must be added here, keeping every previously shipped id mapped.
     */
    public static final class Registry {

        private Registry() {}

        /** Rebuilds a pipeline from the ids recorded in a column's metadata. */
        public static NumericPipeline rebuild(byte terminalId, byte[] transformIds, int blockSize) {
            BlockTransform[] transforms = new BlockTransform[transformIds.length];
            for (int i = 0; i < transformIds.length; i++) {
                transforms[i] = transform(transformIds[i]);
            }
            return new NumericPipeline(transforms, terminal(terminalId, blockSize));
        }

        private static BlockTransform transform(byte id) {
            return switch (id) {
                case DeltaTransform.ID -> new DeltaTransform();
                case OffsetTransform.ID -> new OffsetTransform();
                case GcdTransform.ID -> new GcdTransform();
                default -> throw new IllegalArgumentException("unknown block transform id [" + id + "]");
            };
        }

        private static BlockTerminal terminal(byte id, int blockSize) {
            return switch (id) {
                case ForTerminal.ID -> new ForTerminal(blockSize);
                default -> throw new IllegalArgumentException("unknown block terminal id [" + id + "]");
            };
        }
    }
}
