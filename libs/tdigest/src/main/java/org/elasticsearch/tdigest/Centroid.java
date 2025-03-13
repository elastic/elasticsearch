/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single centroid which represents a number of data points.
 */
public final class Centroid implements Comparable<Centroid> {
    private static final AtomicInteger uniqueCount = new AtomicInteger(1);

    private double centroid = 0;
    private long count = 0;

    // The ID is transient because it must be unique within a given JVM. A new
    // ID should be generated from uniqueCount when a Centroid is deserialized.
    private transient int id;

    private Centroid() {
        id = uniqueCount.getAndIncrement();
    }

    public Centroid(double x) {
        this();
        start(x, 1, uniqueCount.getAndIncrement());
    }

    public Centroid(double x, long w) {
        this();
        start(x, w, uniqueCount.getAndIncrement());
    }

    public Centroid(double x, long w, int id) {
        this();
        start(x, w, id);
    }

    private void start(double x, long w, int id) {
        this.id = id;
        add(x, w);
    }

    public void add(double x, long w) {
        count += w;
        centroid += w * (x - centroid) / count;
    }

    public double mean() {
        return centroid;
    }

    public long count() {
        return count;
    }

    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "Centroid{" + "centroid=" + centroid + ", count=" + count + '}';
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Centroid == false) {
            return false;
        }
        return id == ((Centroid) o).id;
    }

    @Override
    public int compareTo(Centroid o) {
        int r = Double.compare(centroid, o.centroid);
        if (r == 0) {
            r = id - o.id;
        }
        return r;
    }
}
