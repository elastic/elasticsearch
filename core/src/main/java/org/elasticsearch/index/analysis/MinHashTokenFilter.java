package org.elasticsearch.index.analysis;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;

/**
 * Consumes all tokens and emits a set of "minimum hashes", which can be used
 * for similarity comparisons or clustering.
 */
public class MinHashTokenFilter extends TokenFilter {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private int currentPosition;
    private int[] minHashes;

    private static final int[] xorList = {1776307437,1362611959,578040592,444677783,-1468350538,1065497345,476516387,-1150432725,-32553513,202985346,-520256914,-1021791600,1398563721,-632703196,2067011261,260307261,668345640,-301234798,595776579,-560005079,419781730,935472260,-828211474,-456241981,1976420839,-314396499,-1950548137,511222864,-886766967,1241363781,644154611,-454189829,1618568518,-1166467447,-2141665627,84681584,-818755544,-1808672001,610545784,-1920097362,-1630385755,-1366485383,1618298371,1141859413,-938524288,-232256595,-1368579955,-1961625121,-841673731,1523195865,-1530547177,-430431047,-1379363961,1472130856,168110893,-1632899023,814400923,-67643081,-662281233,1117177417,-562077426,1032837633,-649080591,1642897463,-387073035,22893223,70226167,1558424254,-1750881559,-1144535182,1891627172,1788202751,676458057,-1746448170,-1648027094,-1604054212,-1911850598,1772495029,1282957449,1692623317,-301405368,-759670346,-1884095397,-855853986,-1790079248,-1416886022,-1671659898,77310248,-320075646,1133086524,-1936847922,-1265513383,113485981,548483603,315024764,1178055833,-2104204527,1161488314,439706329,-377850679};

    /**
     *
     * @param input     The source of tokens to generate a minhash for
     * @param numHashes The number of independent hashes to use.  More hashes will
     *                  generate more tokens, which allows better similarity/clustering
     *                  accuracy (at the expense of more hashing and larger storage)
     */
    public MinHashTokenFilter(TokenStream input, int numHashes) {
        super(input);
        if (numHashes < 1) {
            throw new IllegalArgumentException("Must use one or more hashes for MinHash Token Filter");
        }
        if (numHashes > 100) {
            throw new IllegalArgumentException("Cannot use more than 100 hashes for MinHash Token Filter");
        }
        this.minHashes = new int[numHashes];
        Arrays.fill(this.minHashes, Integer.MAX_VALUE);
        this.currentPosition = -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean incrementToken() throws IOException {
        if (currentPosition == -1) {
            buildMinHash();
            currentPosition = 0;
        }

        if (currentPosition < minHashes.length) {
            String token = currentPosition + "_" + Integer.toHexString(minHashes[currentPosition]);
            input.clearAttributes();
            termAttribute.setEmpty().append(token);
            posLenAtt.setPositionLength(token.length());
            posIncrAtt.setPositionIncrement(currentPosition);
            typeAtt.setType("fingerprint");
            //TODO offset?

            currentPosition += 1;
            return true;
        }
        return false;
    }

    /**
     * Generates the set of minimum hashes for the token stream
     * @throws IOException
     */
    private void buildMinHash() throws IOException {
        while (input.incrementToken()) {

            int hash = Murmur3HashFunction.hash(termAttribute.toString());
            minHashes[0] = Math.min(minHashes[0], hash);
            // Start at 1 because we can use the original hash for 0
            for (int i = 1; i < this.minHashes.length; i++) {
                int rotatedHash = Integer.rotateRight(hash, i * 2);
                rotatedHash ^= xorList[i - 1];
                minHashes[i] = Math.min(minHashes[i], rotatedHash);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() throws IOException {
        super.reset();
        minHashes = new int[minHashes.length];
        Arrays.fill(this.minHashes, Integer.MAX_VALUE);
        this.currentPosition = -1;
    }

}
