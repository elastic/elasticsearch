package org.elasticsearch.common.lucene.document;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.StoredFieldVisitor;

public abstract class BaseFieldVisitor extends StoredFieldVisitor {

    // LUCENE 4 UPGRADE: Added for now to make everything work. Want to make use of Document as less as possible.
    public abstract Document createDocument();

}
