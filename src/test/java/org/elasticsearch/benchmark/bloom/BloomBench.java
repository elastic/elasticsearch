package org.elasticsearch.benchmark.bloom;

import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.BloomFilter;

import java.security.SecureRandom;

/**
 */
public class BloomBench {

    public static void main(String[] args) throws Exception {
        SecureRandom random = new SecureRandom();
        final int ELEMENTS = (int) SizeValue.parseSizeValue("1m").singles();
        final double fpp = 0.01;
        BloomFilter gFilter = BloomFilter.create(ELEMENTS, fpp);
        System.out.println("G SIZE: " + new ByteSizeValue(gFilter.getSizeInBytes()));

        FuzzySet lFilter = FuzzySet.createSetBasedOnMaxMemory((int) gFilter.getSizeInBytes());
        //FuzzySet lFilter = FuzzySet.createSetBasedOnQuality(ELEMENTS, 0.97f);

        for (int i = 0; i < ELEMENTS; i++) {
            BytesRef bytesRef = new BytesRef(Strings.randomBase64UUID(random));
            gFilter.put(bytesRef);
            lFilter.addValue(bytesRef);
        }

        int lFalse = 0;
        int gFalse = 0;
        for (int i = 0; i < ELEMENTS; i++) {
            BytesRef bytesRef = new BytesRef(Strings.randomBase64UUID(random));
            if (gFilter.mightContain(bytesRef)) {
                gFalse++;
            }
            if (lFilter.contains(bytesRef) == FuzzySet.ContainsResult.MAYBE) {
                lFalse++;
            }
        }
        System.out.println("Failed positives, g[" + gFalse + "], l[" + lFalse + "]");
    }
}
