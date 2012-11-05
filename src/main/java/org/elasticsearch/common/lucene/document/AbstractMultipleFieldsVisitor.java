package org.elasticsearch.common.lucene.document;

import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public abstract class AbstractMultipleFieldsVisitor extends BaseFieldVisitor {

    protected Document doc = new Document();

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        doc.add(new StoredField(fieldInfo.name, value));
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        final FieldType ft = new FieldType(TextField.TYPE_STORED);
        ft.setStoreTermVectors(fieldInfo.hasVectors());
        ft.setIndexed(fieldInfo.isIndexed());
        ft.setOmitNorms(fieldInfo.omitsNorms());
        ft.setIndexOptions(fieldInfo.getIndexOptions());
        doc.add(new Field(fieldInfo.name, value, ft));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {
        doc.add(new StoredField(fieldInfo.name, value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        doc.add(new StoredField(fieldInfo.name, value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
        doc.add(new StoredField(fieldInfo.name, value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
        doc.add(new StoredField(fieldInfo.name, value));
    }

    @Override
    public Document createDocument() {
        return doc;
    }

    @Override
    public void reset() {
        if (!doc.getFields().isEmpty()) {
            doc = new Document();
        }
    }
}
