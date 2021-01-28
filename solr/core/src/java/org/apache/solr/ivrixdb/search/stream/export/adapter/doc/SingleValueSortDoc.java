
package org.apache.solr.ivrixdb.search.stream.export.adapter.doc;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;

public class SingleValueSortDoc extends SortDoc {

  protected SortValue value1;

  public SortValue getSortValue(String field) {
    if (value1.getField().equals(field)) {
      return value1;
    }
    return null;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.ord = context.ord;
    this.docBase = context.docBase;
    value1.setNextReader(context);
  }

  public void reset() {
    this.docId = -1;
    this.docBase = -1;
    this.value1.reset();
  }

  public void setValues(int docId) throws IOException {
    this.docId = docId;
    value1.setCurrentValue(docId);
  }

  public void setValues(SortDoc sortDoc) {
    this.docId = sortDoc.docId;
    this.ord = sortDoc.ord;
    this.docBase = sortDoc.docBase;
    value1.setCurrentValue(((SingleValueSortDoc)sortDoc).value1);
  }

  public SingleValueSortDoc(SortValue value1) {
    super();
    this.value1 = value1;
  }

  public SortDoc copy() {
    return new SingleValueSortDoc(value1.copy());
  }

  public boolean lessThan(Object o) {
    SingleValueSortDoc sd = (SingleValueSortDoc)o;
    int comp = value1.compareTo(sd.value1);
    if(comp == -1) {
      return true;
    } else if (comp == 1) {
      return false;
    } else {
      return docId+docBase > sd.docId+sd.docBase;
    }
  }

  public int compareTo(Object o) {
    SingleValueSortDoc sd = (SingleValueSortDoc)o;
    return value1.compareTo(sd.value1);
  }

  public String toString() {
    return docId+":"+value1.toString();
  }

}
