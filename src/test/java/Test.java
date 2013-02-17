import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.primitives.Longs;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;

import java.lang.reflect.Field;

/**
 * Created with IntelliJ IDEA.
 * User: kimchy
 * Date: 2/17/13
 * Time: 11:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        final int ELEMENTS = (int) SizeValue.parseSizeValue("1m").singles();
        final double fpp = 0.03;

        BloomFilter<BytesRef> gFilter = BloomFilter.create(new Funnel<BytesRef>() {
            @Override
            public void funnel(BytesRef from, PrimitiveSink into) {
                into.putBytes(from.bytes, from.offset, from.length);
            }
        }, ELEMENTS, fpp);

        Field bitsF = gFilter.getClass().getDeclaredField("bits");
        bitsF.setAccessible(true);
        Object bits = bitsF.get(gFilter);
        Field dataF = bits.getClass().getDeclaredField("data");
        dataF.setAccessible(true);
        long[] data = (long[]) dataF.get(bits);

        System.out.println("SIZE: " + new ByteSizeValue(data.length * Longs.BYTES));

        //FuzzySet lFilter = FuzzySet.createSetBasedOnMaxMemory(data.length * Longs.BYTES);
        FuzzySet lFilter = FuzzySet.createSetBasedOnQuality(ELEMENTS, 0.9f);

        for (int i = 0; i < ELEMENTS; i++) {
            BytesRef bytesRef = new BytesRef(UUID.randomBase64UUID());
            gFilter.put(bytesRef);
            lFilter.addValue(bytesRef);
        }

        lFilter = lFilter.downsize(0.1f);

        int lFalse = 0;
        int gFalse = 0;
        for (int i = 0; i < ELEMENTS; i++) {
            BytesRef bytesRef = new BytesRef(UUID.randomBase64UUID());
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
