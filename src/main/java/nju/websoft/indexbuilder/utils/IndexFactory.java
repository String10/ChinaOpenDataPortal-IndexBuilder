package nju.websoft.indexbuilder.utils;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.stereotype.Component;

@Component
public class IndexFactory {

    public IndexWriter indexWriter = null;
    public Integer commitCounter = 0;

    public void init(String storePath, Analyzer analyzer) {
        try {
            Directory directory = MMapDirectory.open(Paths.get(storePath));
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void commitDocument(Document document) {
        try {
            indexWriter.addDocument(document);
            commitCounter++;
            indexWriter.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeIndexWriter() {
        try {
            indexWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
