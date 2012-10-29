package org.elasticsearch.common.lucene.document;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.StoredFieldVisitor;

public abstract class BaseFieldVisitor extends StoredFieldVisitor {

    // LUCENE 4 UPGRADE: Some field visitors need to be cleared before they can be reused. Maybe a better way.
    public abstract void reset();

    // LUCENE 4 UPGRADE: Added for now to make everything work. Want to make use of Document as less as possible.
    public abstract Document createDocument();

}
