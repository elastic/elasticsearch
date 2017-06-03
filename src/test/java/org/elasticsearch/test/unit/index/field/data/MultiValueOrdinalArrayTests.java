package org.elasticsearch.test.unit.index.field.data;

import gnu.trove.list.array.TIntArrayList;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.MultiValueOrdinalArray;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MultiValueOrdinalArrayTests {

   protected int STORAGE_SIZE = 8;

   class smallMultiValueOrdinalArray extends MultiValueOrdinalArray {

      public smallMultiValueOrdinalArray(int[][] ordinalToStore) {
         super(ordinalToStore,STORAGE_SIZE);
      }
   }

   protected TIntArrayList collectOrdinals(MultiValueOrdinalArray a,int docId) {
      final TIntArrayList ol = new TIntArrayList();
      a.forEachOrdinalInDoc(docId, new FieldData.OrdinalInDocProc() {
         @Override
         public void onOrdinal(int docId, int ordinal) {
            ol.add(ordinal);
         }
      });
      return ol;
   }

   protected void regressToArray(ArrayList<int[]> ordinalsPerDoc) {
      // invert ordinalsPerDoc to the structure MultiValueOrdinalArray expects

      smallMultiValueOrdinalArray a = getSmallMultiValueOrdinalArray(ordinalsPerDoc);

      for (int doc=0;doc<ordinalsPerDoc.size();doc++) {
         assertThat(collectOrdinals(a, doc),
                 equalTo(new TIntArrayList(ordinalsPerDoc.get(doc))));
      }
   }

   private smallMultiValueOrdinalArray getSmallMultiValueOrdinalArray(ArrayList<int[]> ordinalsPerDoc) {
      int maxLength = 0;
      for (int[] ol: ordinalsPerDoc) {
         if (maxLength < ol.length) maxLength =  ol.length;
      }

      int [][] ordinalArray= new int[maxLength][ordinalsPerDoc.size()];
      for (int doc=0;doc<ordinalsPerDoc.size();doc++) {
         for (int o=0;o<ordinalsPerDoc.get(doc).length;o++)
            ordinalArray[o][doc]=ordinalsPerDoc.get(doc)[o];
      }

      return new smallMultiValueOrdinalArray(ordinalArray);
   }


   @Test
   public void testSingleValue() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {2});
      o.add(new int[] {3});

      regressToArray(o);
   }

   @Test
   public void testSingleDocMultiValue() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1,2});
      regressToArray(o);
   }


   @Test
   public void testStorageOverflow() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6});
      o.add(new int[] {3});
      regressToArray(o);
   }


   @Test
   public void testComputeSizeInBytes() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6});
      smallMultiValueOrdinalArray a = getSmallMultiValueOrdinalArray(o);
      assertThat(a.computeSizeInBytes(),greaterThan(9L*RamUsage.NUM_BYTES_INT));
   }

   @Test
   public void testHasValue() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6});
      smallMultiValueOrdinalArray a = getSmallMultiValueOrdinalArray(o);
      assertThat(a.hasValue(0),equalTo(true));
      assertThat(a.hasValue(1),equalTo(false));
      assertThat(a.hasValue(2),equalTo(true));
      assertThat(a.hasValue(3),equalTo(true));
   }

   @Test(expectedExceptions = { ElasticSearchException.class } )
   public void testImpossibleStorageOverflow() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6,7,8});
      o.add(new int[] {3});
      regressToArray(o);
   }

}
