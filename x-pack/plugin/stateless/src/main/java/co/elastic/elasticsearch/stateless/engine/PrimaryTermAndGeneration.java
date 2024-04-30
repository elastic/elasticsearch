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

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Comparator;

public record PrimaryTermAndGeneration(long primaryTerm, long generation) implements Writeable, Comparable<PrimaryTermAndGeneration> {

    private static final Comparator<PrimaryTermAndGeneration> COMPARATOR = Comparator.comparing(PrimaryTermAndGeneration::primaryTerm)
        .thenComparing(PrimaryTermAndGeneration::generation);

    public static final PrimaryTermAndGeneration ZERO = new PrimaryTermAndGeneration(0, 0);

    public PrimaryTermAndGeneration(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
    }

    @Override
    public String toString() {
        return "[primary term=" + primaryTerm + ", generation=" + generation + ']';
    }

    @Override
    public int compareTo(PrimaryTermAndGeneration other) {
        return COMPARATOR.compare(this, other);
    }

    public boolean after(PrimaryTermAndGeneration other) {
        return compareTo(other) > 0;
    }

    public boolean onOrAfter(PrimaryTermAndGeneration other) {
        return compareTo(other) >= 0;
    }

    public boolean before(PrimaryTermAndGeneration other) {
        return compareTo(other) < 0;
    }

    public boolean onOrBefore(PrimaryTermAndGeneration other) {
        return compareTo(other) <= 0;
    }

    public static PrimaryTermAndGeneration max(PrimaryTermAndGeneration a, PrimaryTermAndGeneration b) {
        return a.onOrAfter(b) ? a : b;
    }
}
