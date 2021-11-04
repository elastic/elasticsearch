package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ShortDocValuesFieldTests extends ESTestCase {
    public void testShortField() throws IOException {
        short[][] values = new short[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new short[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomShort();
            }
        }

        ShortDocValuesField shortField = wrap(values);

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            shortField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], shortField.get(Short.MIN_VALUE));
                assertEquals(values[d][0], shortField.get(0, Short.MIN_VALUE));
            }
            int count = 0;
            for (int ignored : shortField) {
                count++;
            }
            assertEquals(count, shortField.size());
            assertEquals(values[d].length, shortField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], shortField.get(i, Short.MIN_VALUE));
            }
        }
    }

    private ShortDocValuesField wrap(short[][] values) {
        return new ShortDocValuesField(new AbstractSortedNumericDocValues() {
            short[] current;
            int i;

            @Override
            public boolean advanceExact(int target) throws IOException {
                i = 0;
                current = values[target];
                return current.length > 0;
            }

            @Override
            public long nextValue() throws IOException {
                return current[i++];
            }

            @Override
            public int docValueCount() {
                return current.length;
            }
        }, "myfield");
    }
}
