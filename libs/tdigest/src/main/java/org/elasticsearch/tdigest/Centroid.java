/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single centroid which represents a number of data points.
 */
public class Centroid implements Comparable<Centroid> {
    private static final AtomicInteger uniqueCount = new AtomicInteger(1);

    private double centroid = 0;
    private int count = 0;

    // The ID is transient because it must be unique within a given JVM. A new
    // ID should be generated from uniqueCount when a Centroid is deserialized.
    private transient int id;

    private List<Double> actualData = null;

    private Centroid(boolean record) {
        id = uniqueCount.getAndIncrement();
        if (record) {
            actualData = new ArrayList<>();
        }
    }

    public Centroid(double x) {
        this(false);
        start(x, 1, uniqueCount.getAndIncrement());
    }

    public Centroid(double x, int w) {
        this(false);
        start(x, w, uniqueCount.getAndIncrement());
    }

    public Centroid(double x, int w, int id) {
        this(false);
        start(x, w, id);
    }

    public Centroid(double x, int id, boolean record) {
        this(record);
        start(x, 1, id);
    }

    Centroid(double x, int w, List<Double> data) {
        this(x, w);
        actualData = data;
    }

    private void start(double x, int w, int id) {
        this.id = id;
        add(x, w);
    }

    public void add(double x, int w) {
        if (actualData != null) {
            actualData.add(x);
        }
        count += w;
        centroid += w * (x - centroid) / count;
    }

    public double mean() {
        return centroid;
    }

    public int count() {
        return count;
    }

    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "Centroid{" +
                "centroid=" + centroid +
                ", count=" + count +
                '}';
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

    public List<Double> data() {
        return actualData;
    }

    @SuppressWarnings("WeakerAccess")
    public void insertData(double x) {
        if (actualData == null) {
            actualData = new ArrayList<>();
        }
        actualData.add(x);
    }

    public static Centroid createWeighted(double x, int w, Iterable<? extends Double> data) {
        Centroid r = new Centroid(data != null);
        r.add(x, w, data);
        return r;
    }

    public void add(double x, int w, Iterable<? extends Double> data) {
        if (actualData != null) {
            if (data != null) {
                for (Double old : data) {
                    actualData.add(old);
                }
            } else {
                actualData.add(x);
            }
        }
        centroid = AbstractTDigest.weightedAverage(centroid, count, x, w);
        count += w;
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        id = uniqueCount.getAndIncrement();
    }
}
