package org.elasticsearch.index.field.data;


import gnu.trove.list.array.TIntArrayList;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.ThreadLocals;

import java.util.ArrayList;

/**
 * a specialized container to hold ordinals for {@link FieldData }
 */
public class MultiValueOrdinalArray {

   /*
     A few words on how this works:
     - The goal of this class is to provide an efficient storage

     every document contains an entry in the firstLevel entry
     - an entry of 0 means no ordinals
     - a positive entry means a single ordinal for this document
     - a negative entry means a pointer to (implemented as an offset into) storage data array(s)
     - a storage array entry is a sequence of positive ordinal, terminated by a negative ordinal. All of these
       are associated with the document.

     - the class uses multiple storage arrays to avoid allocating huge array which need big continuous memory.
     - the pointer to a storage array contains two parts: the high bits are an indication which storage array is used.
       the lower bits are an offset into that array.

     - Memory consumption:
         - d0 = set docs with no ordinals
         - d1 = set docs with one ordinal
         - d2 = set of docs with two ordinals or more
         - o2 = all the ordinals for docs in d2

           -> (#d1+#d0)*INT32+ (#d2+#o2)*INT32 -> (#documents + #ordinals)*INT32



    */

   protected ESLogger logger = Loggers.getLogger(getClass());

   protected final int MAX_STORAGE_SIZE_SHIFT;
   protected final int MAX_STORAGE_SIZE;

   protected final int[] firstLevel;
   protected final int[][] storageArrays;

   public MultiValueOrdinalArray(int[][] ordinalToStore) {
      this(ordinalToStore,(1 << 26) / RamUsage.NUM_BYTES_INT); // storage array of 64MB
   }

   protected MultiValueOrdinalArray(int[][] ordinalToStore, int max_storage_size) {
      int shift = 0;

      // not need to over allocated.
      if (ordinalToStore.length * ordinalToStore[0].length < max_storage_size)
         max_storage_size = (ordinalToStore.length * ordinalToStore[0].length) + 1;

      MAX_STORAGE_SIZE = max_storage_size;
      max_storage_size--; // array is 0-based, remove one for maximum possible index
      while (max_storage_size > 0) {
         shift++;
         max_storage_size = max_storage_size >> 1;
      }

      MAX_STORAGE_SIZE_SHIFT = shift;

      ArrayList<int[]> storageArrays = new ArrayList<int[]>();

      // temporary storage for doc ordinals
      TIntArrayList docOrdinals = new TIntArrayList(ordinalToStore.length);

      TIntArrayList curStorageArray = new TIntArrayList(MAX_STORAGE_SIZE);
      int curStorageArrayIndex = 0;

      // Two things about this:
      // 1) First array must start with 1 as 0 pointer means no value.
      // 2) Always points to the next usable place
      int curOffsetWithInStorage = 1;
      curStorageArray.add(Integer.MIN_VALUE); // first place is wasted.
      int maxDoc = ordinalToStore[0].length;

      firstLevel = new int[maxDoc];

      for (int curDoc=0;curDoc < maxDoc; curDoc++) {
         docOrdinals.clear(ordinalToStore.length);
         for (int[] anOrdinalToStore : ordinalToStore) {
            int o = anOrdinalToStore[curDoc];
            if (o == 0) {
               break;
            }
            docOrdinals.add(o);
         }

         switch (docOrdinals.size()) {
            case 0:
               break; // nothing to do
            case 1:
               firstLevel[curDoc] = docOrdinals.get(0);
               break;
            default:


               if ((curOffsetWithInStorage + docOrdinals.size()) > MAX_STORAGE_SIZE) {
                  if (docOrdinals.size() > MAX_STORAGE_SIZE-1) {
                     throw new ElasticSearchException(
                             String.format("Number of values for doc %s has a exceeded the maximum allowed "+
                                     "(got %s values, max %s)",
                                     curDoc,docOrdinals.size(),MAX_STORAGE_SIZE-1));
                  }

                  curStorageArrayIndex++;
                  logger.debug("Allocating a new storage array. {} so far.",curStorageArrayIndex);

                  storageArrays.add(curStorageArray.toArray());
                  curOffsetWithInStorage=1; // for pointer consistency waste a slot.
                  curStorageArray.clear(MAX_STORAGE_SIZE);
                  curStorageArray.add(Integer.MIN_VALUE); // first place is wasted.
               }

               firstLevel[curDoc] = -((curStorageArrayIndex << MAX_STORAGE_SIZE_SHIFT) + curOffsetWithInStorage);

               int j=0;
               for(;j<docOrdinals.size()-1;j++) {
                  curStorageArray.add(docOrdinals.get(j));
                  curOffsetWithInStorage++;
               }
               // mark last with a negative value
               curStorageArray.add(-docOrdinals.get(j));
               curOffsetWithInStorage++;
         }
      }

      // all done. populate final storage space
      this.storageArrays = new int[storageArrays.size()+1][];
      for (int i = 0; i < storageArrays.size(); i++) {
         this.storageArrays[i] = storageArrays.get(i);
      }
      this.storageArrays[storageArrays.size()] = curStorageArray.toArray();

      logger.debug("Ordinal array loaded. %s docs, %s secondary storage arrays. Memory signature: %sKB",
              this.firstLevel.length,this.storageArrays.length,computeSizeInBytes()/1024);
   }

   public long computeSizeInBytes() {
      long size = RamUsage.NUM_BYTES_ARRAY_HEADER + firstLevel.length * RamUsage.NUM_BYTES_INT;
      size += RamUsage.NUM_BYTES_ARRAY_HEADER; // for the top level storagearray
      for (int[] sa : storageArrays) {
         size += RamUsage.NUM_BYTES_ARRAY_HEADER + RamUsage.NUM_BYTES_INT * sa.length;
      }
      size += RamUsage.NUM_BYTES_INT * 2; // constants
      size += RamUsage.NUM_BYTES_OBJECT_REF; // logger

      return size;
   }

   public boolean hasValue(int docId) {
      return firstLevel[docId] != 0;
   }

   public void forEachOrdinalInDoc(int docId, FieldData.OrdinalInDocProc proc) {

      OrdinalIterator iter = getOrdinalIteratorForDoc(docId);

      int o = iter.getNextOrdinal();
      if (o==0) {
         proc.onOrdinal(docId, o); // first one is special as we need to communicate 0 if nothing is found
         return;
      }

      while (o != 0) {
         proc.onOrdinal(docId,o);
         o = iter.getNextOrdinal();
      }
   }

   public interface OrdinalIterator {
      /**
       * Returns the next ordinal for current docId or 0 when no more ordinals are available.
       */
      public int getNextOrdinal();
   }

   public OrdinalIterator getOrdinalIteratorForDoc(int docId) {
      int ordinalOrPointer = firstLevel[docId];

      if (ordinalOrPointer >= 0) {
         return singleIteratorCache.get().get().init(ordinalOrPointer);
      }

      ordinalOrPointer = -ordinalOrPointer;

      int storageArrayIndex = ordinalOrPointer >> MAX_STORAGE_SIZE_SHIFT;
      int[] storageArray = storageArrays[storageArrayIndex];
      ordinalOrPointer -= storageArrayIndex;

      return multiOrdinalIteratorCache.get().get().init(storageArray,ordinalOrPointer);


   }

   private ThreadLocal<ThreadLocals.CleanableValue<SingleOrdinalIterator>> singleIteratorCache =
           new ThreadLocal<ThreadLocals.CleanableValue<SingleOrdinalIterator>>() {
      @Override
      protected ThreadLocals.CleanableValue<SingleOrdinalIterator> initialValue() {
         return new ThreadLocals.CleanableValue<SingleOrdinalIterator>(new SingleOrdinalIterator());
      }
   };

   private ThreadLocal<ThreadLocals.CleanableValue<MultiOrdinalIterator>> multiOrdinalIteratorCache =
           new ThreadLocal<ThreadLocals.CleanableValue<MultiOrdinalIterator>>() {
              @Override
              protected ThreadLocals.CleanableValue<MultiOrdinalIterator> initialValue() {
                 return new ThreadLocals.CleanableValue<MultiOrdinalIterator>(new MultiOrdinalIterator());
              }
           };

   protected static class SingleOrdinalIterator implements OrdinalIterator {

      private int ordinal;

      public SingleOrdinalIterator init(int ordinal) {
         this.ordinal = ordinal;
         return this;
      }

      public int getNextOrdinal() {
         int i=ordinal;
         ordinal=0; // reset for the next time.
         return i;
      }
   }

   protected static class MultiOrdinalIterator implements OrdinalIterator {

      private int ordinalIndex;
      private int[] storageArray;

      public MultiOrdinalIterator init(int[] storageArray,int ordinalIndex) {
         this.storageArray = storageArray;
         this.ordinalIndex = ordinalIndex;
         return this;
      }


      public int getNextOrdinal() {
         if (ordinalIndex < 0) return 0;
         int ordinal = storageArray[ordinalIndex++];
         if (ordinal<0) {
            // last one.
            ordinal = -ordinal;
            ordinalIndex = -1;
         }
         return ordinal;
      }
   }
}