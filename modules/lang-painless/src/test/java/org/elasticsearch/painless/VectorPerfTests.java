/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.field.vectors.BinaryDenseVector;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVector;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVector;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.KnnDenseVector;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;

public class VectorPerfTests extends ESTestCase {

    public static final int DIMS = 200;
    public static final int ITERS = 2000000;

    public void testKnnVectorsDotProduct() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdva[i] = value;
        }

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.dotProduct(bqv);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.dotProduct(bqv);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.dotProduct(fqv);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.dotProduct(fqv);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.dotProduct(bqv);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.dotProduct(bqv);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.dotProduct(fqv);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.dotProduct(fqv);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testBinaryVectorsDotProduct() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS + 4);
        ByteBuffer fdv = ByteBuffer.allocate(DIMS*4 + 4);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdv.putFloat(value);
            fdva[i] = value;
        }
        bdv.putFloat(DenseVector.getMagnitude(bdv.slice(0, DIMS).array()));
        fdv.putFloat(DenseVector.getMagnitude(fdva));

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.dotProduct(bqv);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.dotProduct(bqv);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.dotProduct(fqv);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.dotProduct(fqv);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.dotProduct(bqv);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.dotProduct(bqv);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.dotProduct(fqv);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.dotProduct(fqv);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testKnnVectorsl1Norm() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdva[i] = value;
        }

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l1Norm(bqv);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l1Norm(bqv);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l1Norm(fqv);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l1Norm(fqv);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l1Norm(bqv);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l1Norm(bqv);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l1Norm(fqv);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l1Norm(fqv);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testBinaryVectorsl1Norm() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS + 4);
        ByteBuffer fdv = ByteBuffer.allocate(DIMS*4 + 4);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdv.putFloat(value);
            fdva[i] = value;
        }
        bdv.putFloat(DenseVector.getMagnitude(bdv.slice(0, DIMS).array()));
        fdv.putFloat(DenseVector.getMagnitude(fdva));

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l1Norm(bqv);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l1Norm(bqv);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l1Norm(fqv);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l1Norm(fqv);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.slice(0, 800).array()), DIMS, Version.CURRENT);
            fbdv.l1Norm(bqv);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.slice(0, 800).array()), DIMS, Version.CURRENT);
            fbdv.l1Norm(bqv);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.slice(0, 800).array()), DIMS, Version.CURRENT);
            fbdv.l1Norm(fqv);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.slice(0, 800).array()), DIMS, Version.CURRENT);
            fbdv.l1Norm(fqv);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testKnnVectorsl2Norm() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdva[i] = value;
        }

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l2Norm(bqv);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l2Norm(bqv);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l2Norm(fqv);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.l2Norm(fqv);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l2Norm(bqv);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l2Norm(bqv);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l2Norm(fqv);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.l2Norm(fqv);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testBinaryVectorsl2Norm() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS + 4);
        ByteBuffer fdv = ByteBuffer.allocate(DIMS*4 + 4);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdv.putFloat(value);
            fdva[i] = value;
        }
        bdv.putFloat(DenseVector.getMagnitude(bdv.slice(0, DIMS).array()));
        fdv.putFloat(DenseVector.getMagnitude(fdva));

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l2Norm(bqv);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l2Norm(bqv);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l2Norm(fqv);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.l2Norm(fqv);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.l2Norm(bqv);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.l2Norm(bqv);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.l2Norm(fqv);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.l2Norm(fqv);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testKnnVectorsCosineSimilarity() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdva[i] = value;
        }

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }
        float bqvm = DenseVector.getMagnitude(bqv);
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] /= DenseVector.getMagnitude(fqv);
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.cosineSimilarity(bqv, bqvm);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.cosineSimilarity(bqv, bqvm);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.cosineSimilarity(fqv, false);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteKnnDenseVector bkdv = new ByteKnnDenseVector(new BytesRef(bdv.array()));
            bkdv.cosineSimilarity(fqv, false);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.cosineSimilarity(bqv, bqvm);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.cosineSimilarity(bqv, bqvm);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.cosineSimilarity(fqv, false);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            KnnDenseVector fkdv = new KnnDenseVector(fdva);
            fkdv.cosineSimilarity(fqv, false);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }

    public void testBinaryVectorsCosineSimilarity() {

        /* --------------- */
        ByteBuffer bdv = ByteBuffer.allocate(DIMS + 4);
        ByteBuffer fdv = ByteBuffer.allocate(DIMS*4 + 4);
        float[] fdva = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            byte value = randomByte();
            bdv.put(value);
            fdv.putFloat(value);
            fdva[i] = value;
        }
        bdv.putFloat(DenseVector.getMagnitude(bdv.slice(0, DIMS).array()));
        fdv.putFloat(DenseVector.getMagnitude(fdva));

        /* --------------- */
        byte[] bqv = randomByteArrayOfLength(DIMS);
        float[] fqv = new float[DIMS];
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] = bqv[i];
        }
        float bqvm = DenseVector.getMagnitude(bqv);
        for (int i = 0; i < DIMS; ++i) {
            fqv[i] /= DenseVector.getMagnitude(fqv);
        }

        /* --------------- */
        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.cosineSimilarity(bqv, bqvm);
        }

        long bbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.cosineSimilarity(bqv, bqvm);
        }

        long bbe = System.nanoTime() - bbs;

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.cosineSimilarity(fqv, false);
        }

        long fbs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            ByteBinaryDenseVector bbdv = new ByteBinaryDenseVector(new BytesRef(bdv.array()), DIMS);
            bbdv.cosineSimilarity(fqv, false);
        }

        long fbe = System.nanoTime() - fbs;

        /* --------------- */

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.cosineSimilarity(bqv, bqvm);
        }

        long bfs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.cosineSimilarity(bqv, bqvm);
        }

        long bfe = System.nanoTime() - bfs;

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.cosineSimilarity(fqv, false);
        }

        long ffs = System.nanoTime();

        for (int i = 0; i < ITERS; ++i) {
            BinaryDenseVector fbdv = new BinaryDenseVector(new BytesRef(fdv.array()), DIMS, Version.CURRENT);
            fbdv.cosineSimilarity(fqv, false);
        }

        long ffe = System.nanoTime() - ffs;

        /* --------------- */
        throw new IllegalArgumentException(bbe + " : " + /*fbe + " : " + bfe + " : "*/ + ffe);
    }
}
