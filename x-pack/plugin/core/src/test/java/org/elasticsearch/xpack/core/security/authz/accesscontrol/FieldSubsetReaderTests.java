/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/** Simple tests for this filterreader */
public class FieldSubsetReaderTests extends ESTestCase {

    /**
     * test filtering two string fields
     */
    public void testIndexed() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Set<String> seenFields = new HashSet<>();
        for (FieldInfo info : segmentReader.getFieldInfos()) {
            seenFields.add(info.name);
        }
        assertEquals(Collections.singleton("fieldA"), seenFields);
        assertNotNull(segmentReader.terms("fieldA"));
        assertNull(segmentReader.terms("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two int points
     */
    public void testPoints() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 points
        Document doc = new Document();
        doc.add(new IntPoint("fieldA", 1));
        doc.add(new IntPoint("fieldB", 2));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        PointValues points = segmentReader.getPointValues("fieldA");
        assertNull(segmentReader.getPointValues("fieldB"));

        // size statistic
        assertEquals(1, points.size());

        // doccount statistic
        assertEquals(1, points.getDocCount());

        // min statistic
        assertNotNull(points.getMinPackedValue());

        // max statistic
        assertNotNull(points.getMaxPackedValue());

        // bytes per dimension
        assertEquals(Integer.BYTES, points.getBytesPerDimension());

        // number of dimensions
        assertEquals(1, points.getNumIndexDimensions());

        // walk the trees: we should see stuff in fieldA
        AtomicBoolean sawDoc = new AtomicBoolean(false);
        points.intersect(new IntersectVisitor() {
            @Override
            public void visit(int docID) throws IOException {
                throw new IllegalStateException("should not get here");
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                sawDoc.set(true);
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return Relation.CELL_CROSSES_QUERY;
            }
        });
        assertTrue(sawDoc.get());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two stored fields (string)
     */
    public void testStoredFieldsString() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", "testA"));
        doc.add(new StoredField("fieldB", "testB"));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals("testA", d2.get("fieldA"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two stored fields (binary)
     */
    public void testStoredFieldsBinary() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", new BytesRef("testA")));
        doc.add(new StoredField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals(new BytesRef("testA"), d2.getBinaryValue("fieldA"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two stored fields (int)
     */
    public void testStoredFieldsInt() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", 1));
        doc.add(new StoredField("fieldB", 2));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals(1, d2.getField("fieldA").numericValue());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two stored fields (long)
     */
    public void testStoredFieldsLong() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", 1L));
        doc.add(new StoredField("fieldB", 2L));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals(1L, d2.getField("fieldA").numericValue());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two stored fields (float)
     */
    public void testStoredFieldsFloat() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", 1F));
        doc.add(new StoredField("fieldB", 2F));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals(1F, d2.getField("fieldA").numericValue());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two stored fields (double)
     */
    public void testStoredFieldsDouble() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", 1D));
        doc.add(new StoredField("fieldB", 2D));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals(1D, d2.getField("fieldA").numericValue());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two vector fields
     */
    public void testVectors() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
        ft.setStoreTermVectors(true);
        doc.add(new Field("fieldA", "testA", ft));
        doc.add(new Field("fieldB", "testB", ft));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        Fields vectors = ir.getTermVectors(0);
        Set<String> seenFields = new HashSet<>();
        for (String field : vectors) {
            seenFields.add(field);
        }
        assertEquals(Collections.singleton("fieldA"), seenFields);

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two text fields
     */
    public void testNorms() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new TextField("fieldA", "test", Field.Store.NO));
        doc.add(new TextField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        assertNotNull(segmentReader.getNormValues("fieldA"));
        assertNull(segmentReader.getNormValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two numeric dv fields
     */
    public void testNumericDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new NumericDocValuesField("fieldA", 1));
        doc.add(new NumericDocValuesField("fieldB", 2));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        NumericDocValues values = segmentReader.getNumericDocValues("fieldA");
        assertNotNull(values);
        assertTrue(values.advanceExact(0));
        assertEquals(1, values.longValue());
        assertNull(segmentReader.getNumericDocValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two binary dv fields
     */
    public void testBinaryDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("fieldA", new BytesRef("testA")));
        doc.add(new BinaryDocValuesField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        BinaryDocValues values = segmentReader.getBinaryDocValues("fieldA");
        assertNotNull(values);
        assertTrue(values.advanceExact(0));
        assertEquals(new BytesRef("testA"), values.binaryValue());
        assertNull(segmentReader.getBinaryDocValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two sorted dv fields
     */
    public void testSortedDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new SortedDocValuesField("fieldA", new BytesRef("testA")));
        doc.add(new SortedDocValuesField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        SortedDocValues values = segmentReader.getSortedDocValues("fieldA");
        assertNotNull(values);
        assertTrue(values.advanceExact(0));
        assertEquals(new BytesRef("testA"), values.binaryValue());
        assertNull(segmentReader.getSortedDocValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two sortedset dv fields
     */
    public void testSortedSetDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("fieldA", new BytesRef("testA")));
        doc.add(new SortedSetDocValuesField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        SortedSetDocValues dv = segmentReader.getSortedSetDocValues("fieldA");
        assertNotNull(dv);
        assertTrue(dv.advanceExact(0));
        assertEquals(0, dv.nextOrd());
        assertEquals(SortedSetDocValues.NO_MORE_ORDS, dv.nextOrd());
        assertEquals(new BytesRef("testA"), dv.lookupOrd(0));
        assertNull(segmentReader.getSortedSetDocValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering two sortednumeric dv fields
     */
    public void testSortedNumericDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("fieldA", 1));
        doc.add(new SortedNumericDocValuesField("fieldB", 2));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        SortedNumericDocValues dv = segmentReader.getSortedNumericDocValues("fieldA");
        assertNotNull(dv);
        assertTrue(dv.advanceExact(0));
        assertEquals(1, dv.docValueCount());
        assertEquals(1, dv.nextValue());
        assertNull(segmentReader.getSortedNumericDocValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test we have correct fieldinfos metadata
     */
    public void testFieldInfos() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        FieldInfos infos = segmentReader.getFieldInfos();
        assertEquals(1, infos.size());
        assertNotNull(infos.fieldInfo("fieldA"));
        assertNull(infos.fieldInfo("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test special handling for _source field.
     */
    public void testSourceFilteringIntegration() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "testA", Field.Store.NO));
        doc.add(new StringField("fieldB", "testB", Field.Store.NO));
        byte bytes[] = "{\"fieldA\":\"testA\", \"fieldB\":\"testB\"}".getBytes(StandardCharsets.UTF_8);
        doc.add(new StoredField(SourceFieldMapper.NAME, bytes, 0, bytes.length));
        iw.addDocument(doc);

        // open reader
        Automaton automaton = Automatons.patterns(Arrays.asList("fieldA", SourceFieldMapper.NAME));
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(automaton));

        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals("{\"fieldA\":\"testA\"}", d2.getBinaryValue(SourceFieldMapper.NAME).utf8ToString());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    public void testSourceFiltering() {
        // include on top-level value
        Map<String, Object> map = new HashMap<>();
        map.put("foo", 3);
        map.put("bar", "baz");

        CharacterRunAutomaton include = new CharacterRunAutomaton(Automata.makeString("foo"));
        Map<String, Object> filtered = FieldSubsetReader.filter(map, include, 0);
        Map<String, Object> expected = new HashMap<>();
        expected.put("foo", 3);

        assertEquals(expected, filtered);

        // include on inner wildcard
        map = new HashMap<>();
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("bar", 42);
        subMap.put("baz", 6);
        map.put("foo", subMap);
        map.put("bar", "baz");

        include = new CharacterRunAutomaton(Automatons.patterns("foo.*"));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        expected.put("foo", subMap);

        assertEquals(expected, filtered);

        // include on leading wildcard
        include = new CharacterRunAutomaton(Automatons.patterns("*.bar"));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        subMap = new HashMap<>();
        subMap.put("bar", 42);
        expected.put("foo", subMap);

        assertEquals(expected, filtered);

        // include on inner value
        include = new CharacterRunAutomaton(Automatons.patterns("foo.bar"));
        filtered = FieldSubsetReader.filter(map, include, 0);

        assertEquals(expected, filtered);

        // exclude on exact value
        include = new CharacterRunAutomaton(Operations.minus(
                Automata.makeAnyString(), Automatons.patterns("foo.bar"),
                Operations.DEFAULT_MAX_DETERMINIZED_STATES));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        expected.put("bar", "baz");
        expected.put("foo", Collections.singletonMap("baz", 6));

        assertEquals(expected, filtered);

        // exclude on wildcard
        include = new CharacterRunAutomaton(Operations.minus(
                Automata.makeAnyString(), Automatons.patterns("foo.*"),
                Operations.DEFAULT_MAX_DETERMINIZED_STATES));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = Collections.singletonMap("bar", "baz");

        assertEquals(expected, filtered);

        // include on inner array
        map = new HashMap<>();
        List<Object> subArray = new ArrayList<>();
        subMap = new HashMap<>();
        subMap.put("bar", 42);
        subMap.put("baz", "foo");
        subArray.add(subMap);
        subArray.add(12);
        map.put("foo", subArray);

        include = new CharacterRunAutomaton(Automatons.patterns("foo.bar"));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        subArray = new ArrayList<>();
        subMap = new HashMap<>();
        subMap.put("bar", 42);
        subArray.add(subMap);
        expected.put("foo", subArray);

        assertEquals(expected, filtered);

        // include on inner array 2
        include = new CharacterRunAutomaton(Automatons.patterns("foo"));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        subArray = new ArrayList<>();
        subArray.add(12);
        expected.put("foo", subArray);

        assertEquals(expected, filtered);

        // exclude on inner array
        include = new CharacterRunAutomaton(Operations.minus(
                Automata.makeAnyString(), Automatons.patterns("foo.baz"),
                Operations.DEFAULT_MAX_DETERMINIZED_STATES));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        subArray = new ArrayList<>();
        subMap = new HashMap<>();
        subMap.put("bar", 42);
        subArray.add(subMap);
        subArray.add(12);
        expected.put("foo", subArray);

        assertEquals(expected, filtered);

        // exclude on inner array 2
        include = new CharacterRunAutomaton(Operations.minus(
                Automata.makeAnyString(), Automatons.patterns("foo"),
                Operations.DEFAULT_MAX_DETERMINIZED_STATES));
        filtered = FieldSubsetReader.filter(map, include, 0);
        expected = new HashMap<>();
        subArray = new ArrayList<>();
        subMap = new HashMap<>();
        subMap.put("bar", 42);
        subMap.put("baz", "foo");
        subArray.add(subMap);
        expected.put("foo", subArray);

        assertEquals(expected, filtered);

        // json array objects that have no matching fields should be left empty instead of being removed:
        // (otherwise nested inner hit source filtering fails with AOOB)
        map = new HashMap<>();
        map.put("foo", "value");
        List<Map<?, ?>> values = new ArrayList<>();
        values.add(Collections.singletonMap("foo", "1"));
        values.add(Collections.singletonMap("baz", "2"));
        map.put("bar", values);

        include = new CharacterRunAutomaton(Automatons.patterns("bar.baz"));
        filtered = FieldSubsetReader.filter(map, include, 0);

        expected = new HashMap<>();
        expected.put("bar", Arrays.asList(new HashMap<>(), Collections.singletonMap("baz", "2")));
        assertEquals(expected, filtered);
    }

    /**
     * test special handling for _field_names field.
     */
    public void testFieldNames() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldA", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldB", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        Set<String> fields = new HashSet<>();
        fields.add("fieldA");
        Automaton automaton = Automatons.patterns(Arrays.asList("fieldA", FieldNamesFieldMapper.NAME));
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(automaton));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Terms terms = segmentReader.terms(FieldNamesFieldMapper.NAME);
        TermsEnum termsEnum = terms.iterator();
        assertEquals(new BytesRef("fieldA"), termsEnum.next());
        assertNull(termsEnum.next());

        // seekExact
        termsEnum = terms.iterator();
        assertTrue(termsEnum.seekExact(new BytesRef("fieldA")));
        assertFalse(termsEnum.seekExact(new BytesRef("fieldB")));

        // seekExact with TermState
        // first, collect TermState from underlying reader
        LeafReader unwrappedReader = FilterDirectoryReader.unwrap(ir).leaves().get(0).reader();
        Terms unwrappedTerms = unwrappedReader.terms(FieldNamesFieldMapper.NAME);
        TermsEnum unwrappedTE = unwrappedTerms.iterator();
        assertTrue(unwrappedTE.seekExact(new BytesRef("fieldB")));
        TermState termState = unwrappedTE.termState();

        // now try and seekExact with it
        TermsEnum badEnum = terms.iterator();
        expectThrows(IllegalStateException.class, () -> badEnum.seekExact(new BytesRef("fieldB"), termState));

        // seekCeil
        termsEnum = terms.iterator();
        assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("fieldA")));
        assertEquals(SeekStatus.NOT_FOUND, termsEnum.seekCeil(new BytesRef("field0000")));
        assertEquals(new BytesRef("fieldA"), termsEnum.term());
        assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("fieldAAA")));
        assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("fieldB")));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test special handling for _field_names field (three fields, to exercise termsenum better)
     */
    public void testFieldNamesThreeFields() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        doc.add(new StringField("fieldC", "test", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldA", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldB", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldC", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        Automaton automaton = Automatons.patterns(Arrays.asList("fieldA", "fieldC", FieldNamesFieldMapper.NAME));
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(automaton));

        // see only two fields
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Terms terms = segmentReader.terms(FieldNamesFieldMapper.NAME);
        TermsEnum termsEnum = terms.iterator();
        assertEquals(new BytesRef("fieldA"), termsEnum.next());
        assertEquals(new BytesRef("fieldC"), termsEnum.next());
        assertNull(termsEnum.next());

        // seekExact
        termsEnum = terms.iterator();
        assertTrue(termsEnum.seekExact(new BytesRef("fieldA")));
        assertFalse(termsEnum.seekExact(new BytesRef("fieldB")));
        assertTrue(termsEnum.seekExact(new BytesRef("fieldC")));

        // seekCeil
        termsEnum = terms.iterator();
        assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("fieldA")));
        assertEquals(SeekStatus.NOT_FOUND, termsEnum.seekCeil(new BytesRef("fieldB")));
        assertEquals(new BytesRef("fieldC"), termsEnum.term());
        assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("fieldD")));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test _field_names where a field is permitted, but doesn't exist in the segment.
     */
    public void testFieldNamesMissing() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldA", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldB", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        Automaton automaton = Automatons.patterns(Arrays.asList("fieldA", "fieldC", FieldNamesFieldMapper.NAME));
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(automaton));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Terms terms = segmentReader.terms(FieldNamesFieldMapper.NAME);

        // seekExact
        TermsEnum termsEnum = terms.iterator();
        assertFalse(termsEnum.seekExact(new BytesRef("fieldC")));

        // seekCeil
        termsEnum = terms.iterator();
        assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("fieldC")));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test where _field_names does not exist
     */
    public void testFieldNamesOldIndex() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);

        // open reader
        Automaton automaton = Automatons.patterns(Arrays.asList("fieldA", SourceFieldMapper.NAME));
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(automaton));

        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        assertNull(segmentReader.terms(FieldNamesFieldMapper.NAME));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /** test that core cache key (needed for NRT) is working */
    public void testCoreCacheKey() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMaxBufferedDocs(100);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add two docs, id:0 and id:1
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        doc.add(idField);
        idField.setStringValue("0");
        iw.addDocument(doc);
        idField.setStringValue("1");
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("id")));
        assertEquals(2, ir.numDocs());
        assertEquals(1, ir.leaves().size());

        // delete id:0 and reopen
        iw.deleteDocuments(new Term("id", "0"));
        DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);

        // we should have the same cache key as before
        assertEquals(1, ir2.numDocs());
        assertEquals(1, ir2.leaves().size());
        assertSame(ir.leaves().get(0).reader().getCoreCacheHelper().getKey(),
                ir2.leaves().get(0).reader().getCoreCacheHelper().getKey());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, ir2, iw, dir);
    }

    /**
     * test filtering the only vector fields
     */
    public void testFilterAwayAllVectors() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
        ft.setStoreTermVectors(true);
        doc.add(new Field("fieldA", "testA", ft));
        doc.add(new StringField("fieldB", "testB", Field.Store.NO)); // no vectors
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldB")));

        // sees no fields
        assertNull(ir.getTermVectors(0));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    /**
     * test filtering an index with no fields
     */
    public void testEmpty() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.addDocument(new Document());

        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(Automata.makeString("fieldA")));

        // see no fields
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Set<String> seenFields = new HashSet<>();
        for (FieldInfo info : segmentReader.getFieldInfos()) {
            seenFields.add(info.name);
        }
        assertEquals(0, seenFields.size());
        assertNull(segmentReader.terms("foo"));

        // see no vectors
        assertNull(segmentReader.getTermVectors(0));

        // see no stored fields
        Document document = segmentReader.document(0);
        assertEquals(0, document.getFields().size());

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }

    public void testWrapTwice() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.close();

        final DirectoryReader directoryReader = FieldSubsetReader.wrap(DirectoryReader.open(dir),
                new CharacterRunAutomaton(Automata.makeString("fieldA")));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FieldSubsetReader.wrap(directoryReader,
                new CharacterRunAutomaton(Automata.makeString("fieldA"))));
        assertThat(e.getMessage(), equalTo("Can't wrap [class org.elasticsearch.xpack.core.security.authz.accesscontrol" +
                ".FieldSubsetReader$FieldSubsetDirectoryReader] twice"));
        directoryReader.close();
        dir.close();
    }

    @SuppressWarnings("unchecked")
    public void testMappingsFilteringDuelWithSourceFiltering() throws Exception {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("index")
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                        .putMapping(MAPPING_TEST_ITEM)).build();

        {
            FieldPermissionsDefinition definition = new FieldPermissionsDefinition(new String[]{"*inner1"}, Strings.EMPTY_ARRAY);
            FieldPermissions fieldPermissions = new FieldPermissions(definition);
            ImmutableOpenMap<String, MappingMetadata> mappings = metadata.findMappings(new String[]{"index"},
                    index -> fieldPermissions::grantsAccessTo, Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
            MappingMetadata index = mappings.get("index");
            Map<String, Object> sourceAsMap = index.getSourceAsMap();
            assertEquals(1, sourceAsMap.size());
            Map<String, Object> properties = (Map<String, Object>) sourceAsMap.get("properties");
            assertEquals(2, properties.size());
            Map<String, Object> objectMapping = (Map<String, Object>) properties.get("object");
            assertEquals(1, objectMapping.size());
            Map<String, Object> objectProperties = (Map<String, Object>) objectMapping.get("properties");
            assertEquals(1, objectProperties.size());
            assertTrue(objectProperties.containsKey("inner1"));
            Map<String, Object> nestedMapping = (Map<String, Object>) properties.get("nested");
            assertEquals(2, nestedMapping.size());
            assertEquals("nested", nestedMapping.get("type"));
            Map<String, Object> nestedProperties = (Map<String, Object>) nestedMapping.get("properties");
            assertEquals(1, nestedProperties.size());
            assertTrue(nestedProperties.containsKey("inner1"));

            Automaton automaton = FieldPermissions.initializePermittedFieldsAutomaton(definition);
            CharacterRunAutomaton include = new CharacterRunAutomaton(automaton);
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), DOC_TEST_ITEM, false);
            Map<String, Object> filtered = FieldSubsetReader.filter(stringObjectMap, include, 0);
            assertEquals(2, filtered.size());
            Map<String, Object> object = (Map<String, Object>)filtered.get("object");
            assertEquals(1, object.size());
            assertTrue(object.containsKey("inner1"));
            List<Map<String, Object>> nested = (List<Map<String, Object>>)filtered.get("nested");
            assertEquals(2, nested.size());
            for (Map<String, Object> objectMap : nested) {
                assertEquals(1, objectMap.size());
                assertTrue(objectMap.containsKey("inner1"));
            }
        }
        {
            FieldPermissionsDefinition definition = new FieldPermissionsDefinition(new String[]{"object*"}, Strings.EMPTY_ARRAY);
            FieldPermissions fieldPermissions = new FieldPermissions(definition);
            ImmutableOpenMap<String, MappingMetadata> mappings = metadata.findMappings(new String[]{"index"},
                    index -> fieldPermissions::grantsAccessTo, Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
            MappingMetadata index = mappings.get("index");
            Map<String, Object> sourceAsMap = index.getSourceAsMap();
            assertEquals(1, sourceAsMap.size());
            Map<String, Object> properties = (Map<String, Object>) sourceAsMap.get("properties");
            assertEquals(1, properties.size());
            Map<String, Object> objectMapping = (Map<String, Object>) properties.get("object");
            assertEquals(1, objectMapping.size());
            Map<String, Object> objectProperties = (Map<String, Object>) objectMapping.get("properties");
            assertEquals(2, objectProperties.size());
            Map<String, Object> inner1 = (Map<String, Object>) objectProperties.get("inner1");
            assertEquals(2, inner1.size());
            assertEquals("text", inner1.get("type"));
            Map<String, Object> inner1Fields = (Map<String, Object>) inner1.get("fields");
            assertEquals(1, inner1Fields.size());
            Map<String, Object> inner1Keyword = (Map<String, Object>) inner1Fields.get("keyword");
            assertEquals(1, inner1Keyword.size());
            assertEquals("keyword", inner1Keyword.get("type"));
            Map<String, Object> inner2 = (Map<String, Object>) objectProperties.get("inner2");
            assertEquals(1, inner2.size());
            assertEquals("keyword", inner2.get("type"));

            Automaton automaton = FieldPermissions.initializePermittedFieldsAutomaton(definition);
            CharacterRunAutomaton include = new CharacterRunAutomaton(automaton);
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), DOC_TEST_ITEM, false);
            Map<String, Object> filtered = FieldSubsetReader.filter(stringObjectMap, include, 0);
            assertEquals(1, filtered.size());
            Map<String, Object> object = (Map<String, Object>)filtered.get("object");
            assertEquals(2, object.size());
            assertTrue(object.containsKey("inner1"));
            assertTrue(object.containsKey("inner2"));
        }
        {
            FieldPermissionsDefinition definition = new FieldPermissionsDefinition(new String[]{"object"}, Strings.EMPTY_ARRAY);
            FieldPermissions fieldPermissions = new FieldPermissions(definition);
            ImmutableOpenMap<String, MappingMetadata> mappings = metadata.findMappings(new String[]{"index"},
                    index -> fieldPermissions::grantsAccessTo, Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
            MappingMetadata index = mappings.get("index");
            Map<String, Object> sourceAsMap = index.getSourceAsMap();
            assertEquals(1, sourceAsMap.size());
            Map<String, Object> properties = (Map<String, Object>) sourceAsMap.get("properties");
            assertEquals(1, properties.size());
            Map<String, Object> objectMapping = (Map<String, Object>) properties.get("object");
            assertEquals(1, objectMapping.size());
            Map<String, Object> objectProperties = (Map<String, Object>) objectMapping.get("properties");
            assertEquals(0, objectProperties.size());

            Automaton automaton = FieldPermissions.initializePermittedFieldsAutomaton(definition);
            CharacterRunAutomaton include = new CharacterRunAutomaton(automaton);
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), DOC_TEST_ITEM, false);
            Map<String, Object> filtered = FieldSubsetReader.filter(stringObjectMap, include, 0);
            //TODO FLS filters out empty objects from source, although they are granted access.
            //When filtering mappings though we keep them.
            assertEquals(0, filtered.size());
            /*assertEquals(1, filtered.size());
            Map<String, Object> object = (Map<String, Object>)filtered.get("object");
            assertEquals(0, object.size());*/
        }
        {
            FieldPermissionsDefinition definition = new FieldPermissionsDefinition(new String[]{"nested.inner2"}, Strings.EMPTY_ARRAY);
            FieldPermissions fieldPermissions = new FieldPermissions(definition);
            ImmutableOpenMap<String, MappingMetadata> mappings = metadata.findMappings(new String[]{"index"},
                    index -> fieldPermissions::grantsAccessTo, Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
            MappingMetadata index = mappings.get("index");
            Map<String, Object> sourceAsMap = index.getSourceAsMap();
            assertEquals(1, sourceAsMap.size());
            Map<String, Object> properties = (Map<String, Object>) sourceAsMap.get("properties");
            assertEquals(1, properties.size());
            Map<String, Object> nestedMapping = (Map<String, Object>) properties.get("nested");
            assertEquals(2, nestedMapping.size());
            assertEquals("nested", nestedMapping.get("type"));
            Map<String, Object> nestedProperties = (Map<String, Object>) nestedMapping.get("properties");
            assertEquals(1, nestedProperties.size());
            assertTrue(nestedProperties.containsKey("inner2"));

            Automaton automaton = FieldPermissions.initializePermittedFieldsAutomaton(definition);
            CharacterRunAutomaton include = new CharacterRunAutomaton(automaton);
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), DOC_TEST_ITEM, false);
            Map<String, Object> filtered = FieldSubsetReader.filter(stringObjectMap, include, 0);
            assertEquals(1, filtered.size());
            List<Map<String, Object>> nested = (List<Map<String, Object>>)filtered.get("nested");
            assertEquals(2, nested.size());
            for (Map<String, Object> objectMap : nested) {
                assertEquals(1, objectMap.size());
                assertTrue(objectMap.containsKey("inner2"));
            }
        }
    }

    public void testProducesStoredFieldsReader() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);
        iw.commit();

        // add document with 2 fields
        doc = new Document();
        doc.add(new StringField("fieldA", "test2", Field.Store.NO));
        doc.add(new StringField("fieldB", "test2", Field.Store.NO));
        iw.addDocument(doc);
        iw.commit();

        // open reader
        Automaton automaton = Automatons.patterns(Arrays.asList("fieldA", SourceFieldMapper.NAME));
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw), new CharacterRunAutomaton(automaton));

        TestUtil.checkReader(ir);
        assertThat(ir.leaves().size(), Matchers.greaterThanOrEqualTo(1));
        for (LeafReaderContext context: ir.leaves()) {
            assertThat(context.reader(), Matchers.instanceOf(SequentialStoredFieldsLeafReader.class));
            SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) context.reader();
            assertNotNull(lf.getSequentialStoredFieldsReader());
        }
        IOUtils.close(ir, iw, dir);
    }

    private static final String DOC_TEST_ITEM = "{\n" +
            "  \"field_text\" : \"text\",\n" +
            "  \"object\" : {\n" +
            "    \"inner1\" : \"text\",\n" +
            "    \"inner2\" : \"keyword\"\n" +
            "  },\n" +
            "  \"nested\" : [\n" +
            "    {\n" +
            "      \"inner1\" : 1,\n" +
            "      \"inner2\" : \"2017/12/12\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"inner1\" : 2,\n" +
            "      \"inner2\" : \"2017/11/11\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private static final String MAPPING_TEST_ITEM = "{\n" +
            "  \"_doc\": {\n" +
            "    \"properties\" : {\n" +
            "      \"field_text\" : {\n" +
            "        \"type\":\"text\"\n" +
            "      },\n" +
            "      \"object\" : {\n" +
            "        \"properties\" : {\n" +
            "          \"inner1\" : {\n" +
            "            \"type\": \"text\",\n" +
            "            \"fields\" : {\n" +
            "              \"keyword\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"inner2\" : {\n" +
            "            \"type\": \"keyword\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      \"nested\" : {\n" +
            "        \"type\":\"nested\",\n" +
            "        \"properties\" : {\n" +
            "          \"inner1\" : {\n" +
            "            \"type\": \"integer\"\n" +
            "          },\n" +
            "          \"inner2\" : {\n" +
            "            \"type\": \"date\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
}
