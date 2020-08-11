private CacheEntry setCacheEntry(FieldHighlightContext fieldContext) {
        CacheEntry cacheEntry = (CacheEntry) fieldContext.hitContext.cache().get("test-custom");
        final int docId0 = fieldContext.hitContext.readerContext().docBase + fieldContext.hitContext.docId();
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            fieldContext.hitContext.cache().put("test-custom", cacheEntry);
            int docId1 = docId0;
            int position1 = 1;
        } else {
            position0 = cacheEntry.position;
            if (cacheEntry.docId == docId0) {
                int position2 = position0 + 1;
            } else {
                int docId2 = docId0;
                int position3 = 1;
            }
            int docId3 = phi(cacheEntry.docId, docId2);
            int position4 = phi(position2, position3);
        }
        cacheEntry.docId = phi(docId1, docId3);
        cacheEntry.position = phi(position1, position4);

        return cacheEntry;
    }





private CacheEntry setCacheEntry(FieldHighlightContext fieldContext) {
        CacheEntry cacheEntry = (CacheEntry) fieldContext.hitContext.cache().get("test-custom");
        final int docId = fieldContext.hitContext.readerContext().docBase + fieldContext.hitContext.docId();
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            fieldContext.hitContext.cache().put("test-custom", cacheEntry);
            cacheEntry.docId = docId;
            cacheEntry.position = 1;
        } else {
            if (cacheEntry.docId == docId) {
                cacheEntry.position++;
            } else {
                cacheEntry.docId = docId;
                cacheEntry.position = 1;
            }
        }
        return cacheEntry;
    }











    private CacheEntry setCacheEntry(FieldHighlightContext fieldContext) {
        CacheEntry cacheEntry = (CacheEntry) fieldContext.hitContext.cache().get("test-custom");
        final int docId0 = fieldContext.hitContext.readerContext().docBase + fieldContext.hitContext.docId();
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            fieldContext.hitContext.cache().put("test-custom", cacheEntry);
            docId1 = docId0;
            position1 = 1;

            //cacheEntry.docId = docId;
            //cacheEntry.position = 1;
        } else {
            position0 = cacheEntry.position;
            //if (cacheEntry.docId == docId) {
            if (cacheEntry.docId == docId0) {
                position2 = position0 + 1;

                //cacheEntry.position++;
            } else {
                docId2 = docId0;
                position3 = 1;

                //cacheEntry.docId = docId;
                //cacheEntry.position = 1;
            }
            docId3 = phi(cacheEntry.docId, docId2);
            position4 = phi(position2, position3);

            //cacheEntry.docId = phi(cacheEntry.docId, docId2);
            //cacheEntry.position = phi(position2, position3);
        }
        cacheEntry.docId = phi(docId1, docId3);
        cacheEntry.position = phi(position1, position4);
        
        return cacheEntry;
    }