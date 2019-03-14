package org.elasticsearch.index.mapper;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class KeyedJsonAtomicFieldData implements AtomicOrdinalsFieldData {

    private final String key;
    private final AtomicOrdinalsFieldData delegate;

    KeyedJsonAtomicFieldData(String key,
                             AtomicOrdinalsFieldData delegate) {
        this.key = key;
        this.delegate = delegate;
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public SortedSetDocValues getOrdinalsValues() {
        SortedSetDocValues values = delegate.getOrdinalsValues();
        return new KeyedJsonDocValues(key, values);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ScriptDocValues<?> getScriptValues() {
        return AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
            .apply(getOrdinalsValues());
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getOrdinalsValues());
    }

    private static class KeyedJsonDocValues extends AbstractSortedSetDocValues {

        private final BytesRef prefix;
        private final SortedSetDocValues delegate;

        KeyedJsonDocValues(String key, SortedSetDocValues delegate) {
            this.prefix = new BytesRef(JsonFieldParser.createKeyedValue(key, ""));
            this.delegate = delegate;
        }

        @Override
        public long getValueCount() {
            return delegate.getValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            BytesRef keyedValue = delegate.lookupOrd(ord);
            int valueLength = keyedValue.length - prefix.length;
            return new BytesRef(keyedValue.bytes, prefix.length, valueLength);
        }

        @Override
        public long nextOrd() throws IOException {
            for (long ord = delegate.nextOrd(); ord != NO_MORE_ORDS; ord = delegate.nextOrd()) {
                if (accepted(ord)) {
                    return ord;
                }
            }
            return NO_MORE_ORDS;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (delegate.advanceExact(target)) {
                for (long ord = delegate.nextOrd(); ord != NO_MORE_ORDS; ord = delegate.nextOrd()) {
                    if (accepted(ord)) {
                        boolean advanced = delegate.advanceExact(target);
                        assert advanced;
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean accepted(long ord) throws IOException {
            BytesRef value = delegate.lookupOrd(ord);
            if (value.length < prefix.length) {
                return false;
            }

            for (int i = 0; i < prefix.length; i++ ) {
                if (value.bytes[value.offset + i] != prefix.bytes[prefix.offset + i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
