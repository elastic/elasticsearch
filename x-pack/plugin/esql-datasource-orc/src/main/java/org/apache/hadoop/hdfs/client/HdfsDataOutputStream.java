/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.hadoop.hdfs.client;

import java.io.OutputStream;
import java.util.EnumSet;

/**
 * Minimal stub replacing the {@code hadoop-hdfs-client} jar (5.3 MB).
 * <p>
 * The original class lives in the Apache Hadoop HDFS Client module
 * ({@code org.apache.hadoop:hadoop-hdfs-client}, Apache License 2.0) and
 * provides HDFS-specific output stream functionality. ORC's
 * {@code HadoopShimsCurrent} (in {@code orc-shims}) references this type
 * statically in its {@code endOutputStreamBlock} method: it checks
 * {@code stream instanceof HdfsDataOutputStream} and, if true, calls
 * {@code hsync(EnumSet.of(SyncFlag.END_BLOCK))}.
 * <p>
 * When the JVM loads {@code HadoopShimsCurrent} — triggered by the ORC
 * <b>reader</b> path via {@code RecordReaderUtils.<clinit>()} →
 * {@code HadoopShimsFactory.get()} — it resolves all class references
 * including {@code HdfsDataOutputStream} and its inner enum {@code SyncFlag}.
 * Without this stub, a {@code NoClassDefFoundError} crashes the ES node on
 * the first ORC read attempt.
 * <p>
 * At runtime, no stream is an instance of this stub, so the {@code instanceof}
 * check in {@code HadoopShimsCurrent} always returns false and the HDFS-specific
 * code path is never executed. This is an independent stub (not a copy of the
 * original class); it only provides the type and enum declarations needed for
 * class loading.
 */
public class HdfsDataOutputStream extends OutputStream {

    public enum SyncFlag {
        UPDATE_LENGTH,
        END_BLOCK
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException("stub");
    }

    public void hsync(EnumSet<SyncFlag> syncFlags) {
        throw new UnsupportedOperationException("stub");
    }
}
