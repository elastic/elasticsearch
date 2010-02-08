/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util.gnu.trove;

import java.util.Arrays;

/*
 * Modified for Trove to use the java.util.Arrays sort/search
 * algorithms instead of those provided with colt.
 */

/**
 * Used to keep hash table capacities prime numbers.
 * Not of interest for users; only for implementors of hashtables.
 * <p/>
 * <p>Choosing prime numbers as hash table capacities is a good idea
 * to keep them working fast, particularly under hash table
 * expansions.
 * <p/>
 * <p>However, JDK 1.2, JGL 3.1 and many other toolkits do nothing to
 * keep capacities prime.  This class provides efficient means to
 * choose prime capacities.
 * <p/>
 * <p>Choosing a prime is <tt>O(log 300)</tt> (binary search in a list
 * of 300 ints).  Memory requirements: 1 KB static memory.
 *
 * @author wolfgang.hoschek@cern.ch
 * @version 1.0, 09/24/99
 */
public final class PrimeFinder {
    /**
     * The largest prime this class can generate; currently equal to
     * <tt>Integer.MAX_VALUE</tt>.
     */
    public static final int largestPrime = Integer.MAX_VALUE; //yes, it is prime.

    /**
     * The prime number list consists of 11 chunks.
     * <p/>
     * Each chunk contains prime numbers.
     * <p/>
     * A chunk starts with a prime P1. The next element is a prime
     * P2. P2 is the smallest prime for which holds: P2 >= 2*P1.
     * <p/>
     * The next element is P3, for which the same holds with respect
     * to P2, and so on.
     * <p/>
     * Chunks are chosen such that for any desired capacity >= 1000
     * the list includes a prime number <= desired capacity * 1.11.
     * <p/>
     * Therefore, primes can be retrieved which are quite close to any
     * desired capacity, which in turn avoids wasting memory.
     * <p/>
     * For example, the list includes
     * 1039,1117,1201,1277,1361,1439,1523,1597,1759,1907,2081.
     * <p/>
     * So if you need a prime >= 1040, you will find a prime <=
     * 1040*1.11=1154.
     * <p/>
     * Chunks are chosen such that they are optimized for a hashtable
     * growthfactor of 2.0;
     * <p/>
     * If your hashtable has such a growthfactor then, after initially
     * "rounding to a prime" upon hashtable construction, it will
     * later expand to prime capacities such that there exist no
     * better primes.
     * <p/>
     * In total these are about 32*10=320 numbers -> 1 KB of static
     * memory needed.
     * <p/>
     * If you are stingy, then delete every second or fourth chunk.
     */

    private static final int[] primeCapacities = {
            //chunk #0
            largestPrime,

            //chunk #1
            5, 11, 23, 47, 97, 197, 397, 797, 1597, 3203, 6421, 12853, 25717, 51437, 102877, 205759,
            411527, 823117, 1646237, 3292489, 6584983, 13169977, 26339969, 52679969, 105359939,
            210719881, 421439783, 842879579, 1685759167,

            //chunk #2
            433, 877, 1759, 3527, 7057, 14143, 28289, 56591, 113189, 226379, 452759, 905551, 1811107,
            3622219, 7244441, 14488931, 28977863, 57955739, 115911563, 231823147, 463646329, 927292699,
            1854585413,

            //chunk #3
            953, 1907, 3821, 7643, 15287, 30577, 61169, 122347, 244703, 489407, 978821, 1957651, 3915341,
            7830701, 15661423, 31322867, 62645741, 125291483, 250582987, 501165979, 1002331963,
            2004663929,

            //chunk #4
            1039, 2081, 4177, 8363, 16729, 33461, 66923, 133853, 267713, 535481, 1070981, 2141977, 4283963,
            8567929, 17135863, 34271747, 68543509, 137087021, 274174111, 548348231, 1096696463,

            //chunk #5
            31, 67, 137, 277, 557, 1117, 2237, 4481, 8963, 17929, 35863, 71741, 143483, 286973, 573953,
            1147921, 2295859, 4591721, 9183457, 18366923, 36733847, 73467739, 146935499, 293871013,
            587742049, 1175484103,

            //chunk #6
            599, 1201, 2411, 4831, 9677, 19373, 38747, 77509, 155027, 310081, 620171, 1240361, 2480729,
            4961459, 9922933, 19845871, 39691759, 79383533, 158767069, 317534141, 635068283, 1270136683,

            //chunk #7
            311, 631, 1277, 2557, 5119, 10243, 20507, 41017, 82037, 164089, 328213, 656429, 1312867,
            2625761, 5251529, 10503061, 21006137, 42012281, 84024581, 168049163, 336098327, 672196673,
            1344393353,

            //chunk #8
            3, 7, 17, 37, 79, 163, 331, 673, 1361, 2729, 5471, 10949, 21911, 43853, 87719, 175447, 350899,
            701819, 1403641, 2807303, 5614657, 11229331, 22458671, 44917381, 89834777, 179669557,
            359339171, 718678369, 1437356741,

            //chunk #9
            43, 89, 179, 359, 719, 1439, 2879, 5779, 11579, 23159, 46327, 92657, 185323, 370661, 741337,
            1482707, 2965421, 5930887, 11861791, 23723597, 47447201, 94894427, 189788857, 379577741,
            759155483, 1518310967,

            //chunk #10
            379, 761, 1523, 3049, 6101, 12203, 24407, 48817, 97649, 195311, 390647, 781301, 1562611,
            3125257, 6250537, 12501169, 25002389, 50004791, 100009607, 200019221, 400038451, 800076929,
            1600153859
    };

    static { //initializer
        // The above prime numbers are formatted for human readability.
        // To find numbers fast, we sort them once and for all.

        Arrays.sort(primeCapacities);
    }

    /**
     * Returns a prime number which is <code>&gt;= desiredCapacity</code>
     * and very close to <code>desiredCapacity</code> (within 11% if
     * <code>desiredCapacity &gt;= 1000</code>).
     *
     * @param desiredCapacity the capacity desired by the user.
     * @return the capacity which should be used for a hashtable.
     */
    public static final int nextPrime(int desiredCapacity) {
        int i = Arrays.binarySearch(primeCapacities, desiredCapacity);
        if (i < 0) {
            // desired capacity not found, choose next prime greater
            // than desired capacity
            i = -i - 1; // remember the semantics of binarySearch...
        }
        return primeCapacities[i];
    }
}
