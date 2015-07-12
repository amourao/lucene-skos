package at.ac.univie.mminf.luceneSKOS.skos.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import at.ac.univie.mminf.luceneSKOS.skos.MeSHEngine;
import at.ac.univie.mminf.luceneSKOS.skos.SKOS;
import at.ac.univie.mminf.luceneSKOS.skos.impl.SKOSEngineImpl.AllDocCollector;

import com.hp.hpl.jena.ontology.AnnotationProperty;
import com.hp.hpl.jena.ontology.ObjectProperty;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

public class MeSHEngineImpl implements MeSHEngine {

	
	/** Records the total number of matches */
	  public static class AllDocCollector extends Collector {
	    private final List<Integer> docs = new ArrayList<Integer>();
	    private int base;
	    
	    @Override
	    public boolean acceptsDocsOutOfOrder() {
	      return true;
	    }
	    
	    @Override
	    public void collect(int doc) throws IOException {
	      docs.add(doc + base);
	    }
	    
	    public List<Integer> getDocs() {
	      return docs;
	    }
	    
	    @Override
	    public void setNextReader(AtomicReaderContext context) throws IOException {
	      base = context.docBase;
	    }
	    
	    @Override
	    public void setScorer(Scorer scorer) throws IOException {
	      // not needed
	    }
	  }
	  
	  protected final Version matchVersion;
	  
	  /*
	   * Static fields used in the Lucene Index
	   */
	  private static final String FIELD_URI = "uri";
	  private static final String FIELD_PREF_LABEL = "pref";
	  private static final String FIELD_ALT_LABEL = "alt";
	  private static final String FIELD_HIDDEN_LABEL = "hidden";
	  protected static final String FIELD_BROADER = "broader";
	  protected static final String FIELD_NARROWER = "narrower";
	  private static final String FIELD_BROADER_TRANSITIVE = "broaderTransitive";
	  private static final String FIELD_NARROWER_TRANSITIVE = "narrowerTransitive";
	  private static final String FIELD_RELATED = "related";
	  
	  /**
	   * The input SKOS model
	   */
	  protected Model skosModel;
	  
	  /**
	   * The location of the concept index
	   */
	  private Directory indexDir;
	  
	  /**
	   * Provides access to the index
	   */
	  private IndexSearcher searcher;
	  
	  /**
	   * The languages to be considered when returning labels.
	   * 
	   * If NULL, all languages are supported
	   */
	  private Set<String> languages;
	  
	  /**
	   * The analyzer used during indexing of / querying for concepts
	   * 
	   * SimpleAnalyzer = LetterTokenizer + LowerCaseFilter
	   */
	  private final Analyzer analyzer;
	  
	  /**
	   * This constructor loads the SKOS model from a given InputStream using the
	   * given serialization language parameter, which must be either N3, RDF/XML,
	   * or TURTLE.
	   * 
	   * @param inputStream
	   *          the input stream
	   * @param lang
	   *          the serialization language
	   * @throws IOException
	   *           if the model cannot be loaded
	   */
	  public MeSHEngineImpl(final Version version, InputStream inputStream,
	      String lang) throws IOException {
	    
	    if (!("N3".equals(lang) || "RDF/XML".equals(lang) || "TURTLE".equals(lang))) {
	      throw new IOException("Invalid RDF serialization format");
	    }
	    
	    matchVersion = version;
	    
	    analyzer = new SimpleAnalyzer(matchVersion);
	    
	    skosModel = ModelFactory.createDefaultModel();
	    
	    skosModel.read(inputStream, null, lang);
	    
	    indexDir = new RAMDirectory();
	    
	    entailSKOSModel();
	    
	    indexSKOSModel();
	    
	    searcher = new IndexSearcher(DirectoryReader.open(indexDir));
	  }
	  
	  /**
	   * Constructor for all label-languages
	   * 
	   * @param filenameOrURI
	   *          the name of the skos file to be loaded
	   * @throws IOException
	   */
	  public MeSHEngineImpl(final Version version, String filenameOrURI)
	      throws IOException {
	    this(version, filenameOrURI, (String[]) null);
	  }
	  
	  /**
	   * This constructor loads the SKOS model from a given filename or URI, starts
	   * the indexing process and sets up the index searcher.
	   * 
	   * @param languages
	   *          the languages to be considered
	   * @param filenameOrURI
	   * @throws IOException
	   */
	  public MeSHEngineImpl(final Version version, String filenameOrURI,
	      String... languages) throws IOException {
	    matchVersion = version;
	    analyzer = new SimpleAnalyzer(matchVersion);
	    
	    String langSig = "";
	    if (languages != null) {
	      this.languages = new TreeSet<String>(Arrays.asList(languages));
	      langSig = "-" + StringUtils.join(this.languages, ".");
	    }
	    
	    String name = FilenameUtils.getName(filenameOrURI);
	    File dir = new File("skosdata/" + name + langSig);
	    indexDir = FSDirectory.open(dir);
	    
	    // TODO: Generate also if source file is modified
	    if (!dir.isDirectory()) {
	      // load the skos model from the given file
	      FileManager fileManager = new FileManager();
	      fileManager.addLocatorFile();
	      fileManager.addLocatorURL();
	      fileManager.addLocatorClassLoader(SKOSEngineImpl.class.getClassLoader());
	      
	      if (FilenameUtils.getExtension(filenameOrURI).equals("zip")) {
	        fileManager.addLocatorZip(filenameOrURI);
	        filenameOrURI = FilenameUtils.getBaseName(filenameOrURI);
	      }
	      
	      skosModel = fileManager.loadModel(filenameOrURI);
	      
	      entailSKOSModel();
	      
	      indexSKOSModel();
	    }
	    
	    searcher = new IndexSearcher(DirectoryReader.open(indexDir));
	  }
	  
	  private void entailSKOSModel() {
	    GraphStore graphStore = GraphStoreFactory.create(skosModel) ;
	    String sparqlQuery = StringUtils.join(new String[]{
	        "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>",
	        "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
	        "INSERT { ?subject rdf:type skos:Concept }",
	        "WHERE {",
	          "{ ?subject skos:prefLabel ?text } UNION",
	          "{ ?subject skos:altLabel ?text } UNION",
	          "{ ?subject skos:hiddenLabel ?text }",
	         "}",
	        }, "\n");
	    UpdateRequest request = UpdateFactory.create(sparqlQuery);
	    UpdateAction.execute(request, graphStore) ;
	    
	        
	    String sparqlQuery1 = StringUtils.join(new String[]{
	        "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>",
	        "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
	        "INSERT { ?subject skos:narrower ?narrower }",
	        "WHERE {",
	          "?narrower skos:broader ?subject .",
	          "?narrower rdf:type skos:Concept",
	         "}",
	        }, "\n");
	    UpdateRequest request1 = UpdateFactory.create(sparqlQuery1);
	    UpdateAction.execute(request1, graphStore) ;
	  }

	  /**
	   * Creates lucene documents from SKOS concept. In order to allow language
	   * restrictions, one document per language is created.
	   */
	  protected Document createDocumentsFromConcept(Resource skos_concept) {
	    Document conceptDoc = new Document();
	    
	    String conceptURI = skos_concept.getURI();
	    if (conceptURI == null) {
	      System.err.println("Error when indexing concept NO_URI.");
	      return null;
	    }
	    
	    Field uriField = new Field(FIELD_URI, conceptURI, StringField.TYPE_STORED);
	    conceptDoc.add(uriField);
	    
	    // store the preferred lexical labels
	    indexAnnotation(skos_concept, conceptDoc, SKOS.prefLabel, FIELD_PREF_LABEL);
	    
	    // store the alternative lexical labels
	    indexAnnotation(skos_concept, conceptDoc, SKOS.altLabel, FIELD_ALT_LABEL);
	    
	    // store the hidden lexical labels
	    indexAnnotation(skos_concept, conceptDoc, SKOS.hiddenLabel,
	        FIELD_HIDDEN_LABEL);
	    
	    // store the URIs of the broader concepts
	    indexObject(skos_concept, conceptDoc, SKOS.broader, FIELD_BROADER);
	    
	    // store the URIs of the broader transitive concepts
	    indexObject(skos_concept, conceptDoc, SKOS.broaderTransitive,
	        FIELD_BROADER_TRANSITIVE);
	    
	    // store the URIs of the narrower concepts
	    indexObject(skos_concept, conceptDoc, SKOS.narrower, FIELD_NARROWER);
	    
	    // store the URIs of the narrower transitive concepts
	    indexObject(skos_concept, conceptDoc, SKOS.narrowerTransitive,
	        FIELD_NARROWER_TRANSITIVE);
	    
	    // store the URIs of the related concepts
	    indexObject(skos_concept, conceptDoc, SKOS.related, FIELD_RELATED);
	    
	    recursiveIndexObject(skos_concept,conceptDoc,SKOS.broader, FIELD_BROADER);
		recursiveIndexObject(skos_concept,conceptDoc,SKOS.narrower, FIELD_NARROWER);
	    
	    return conceptDoc;
	  }
	  
	  @Override
	  public String[] getAltLabels(String conceptURI) throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_ALT_LABEL);
	  }
	  
	  @Override
	  public String[] getAltTerms(String label) throws IOException {
	    List<String> result = new ArrayList<String>();
	    
	    // convert the query to lower-case
	    String queryString = label.toLowerCase();
	    
	    try {
	      String[] conceptURIs = getConcepts(queryString);
	      
	      for (String conceptURI : conceptURIs) {
	        String[] altLabels = getAltLabels(conceptURI);
	        if (altLabels != null) {
	          for (String altLabel : altLabels) {
	            result.add(altLabel);
	          }
	        }
	      }
	    } catch (Exception e) {
	      System.err
	          .println("Error when accessing SKOS Engine.\n" + e.getMessage());
	    }
	    
	    return result.toArray(new String[result.size()]);
	  }
	  
	  @Override
	  public String[] getHiddenLabels(String conceptURI) throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_HIDDEN_LABEL);
	  }
	  
	  @Override
	  public String[] getBroaderConcepts(String conceptURI) throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_BROADER);
	  }
	  
	  @Override
	  public String[] getBroaderLabels(String conceptURI) throws IOException {
	    return getLabels(conceptURI, FIELD_BROADER);
	  }
	  
	  @Override
	  public String[] getBroaderTransitiveConcepts(String conceptURI)
	      throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_BROADER_TRANSITIVE);
	  }
	  
	  @Override
	  public String[] getBroaderTransitiveLabels(String conceptURI)
	      throws IOException {
	    return getLabels(conceptURI, FIELD_BROADER_TRANSITIVE);
	  }
	  
	  @Override
	  public String[] getConcepts(String label) throws IOException {
	    List<String> concepts = new ArrayList<String>();
	    
	    // convert the query to lower-case
	    String queryString = label.toLowerCase();
	    
	    AllDocCollector collector = new AllDocCollector();
	    
	    DisjunctionMaxQuery query = new DisjunctionMaxQuery(0.0f);
	    query.add(new TermQuery(new Term(FIELD_PREF_LABEL, queryString)));
	    query.add(new TermQuery(new Term(FIELD_ALT_LABEL, queryString)));
	    query.add(new TermQuery(new Term(FIELD_HIDDEN_LABEL, queryString)));
	    searcher.search(query, collector);
	    
	    for (Integer hit : collector.getDocs()) {
	      Document doc = searcher.doc(hit);
	      String conceptURI = doc.getValues(FIELD_URI)[0];
	      concepts.add(conceptURI);
	    }
	    
	    return concepts.toArray(new String[concepts.size()]);
	  }
	  
	  protected String[] getLabels(String conceptURI, String field)
	      throws IOException {
	    List<String> labels = new ArrayList<String>();
	    String[] concepts = readConceptFieldValues(conceptURI, field);
	    
	    for (String aConceptURI : concepts) {
	      String[] prefLabels = getPrefLabels(aConceptURI);
	      labels.addAll(Arrays.asList(prefLabels));
	      
	      String[] altLabels = getAltLabels(aConceptURI);
	      labels.addAll(Arrays.asList(altLabels));
	    }
	    
	    return labels.toArray(new String[labels.size()]);
	  }
	  
	  @Override
	  public String[] getNarrowerConcepts(String conceptURI) throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_NARROWER);
	  }
	  
	  @Override
	  public String[] getNarrowerLabels(String conceptURI) throws IOException {
	    return getLabels(conceptURI, FIELD_NARROWER);
	  }
	  
	  @Override
	  public String[] getNarrowerTransitiveConcepts(String conceptURI)
	      throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_NARROWER_TRANSITIVE);
	  }
	  
	  @Override
	  public String[] getNarrowerTransitiveLabels(String conceptURI)
	      throws IOException {
	    return getLabels(conceptURI, FIELD_NARROWER_TRANSITIVE);
	  }
	  
	  @Override
	  public String[] getPrefLabels(String conceptURI) throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_PREF_LABEL);
	  }
	  
	  @Override
	  public String[] getRelatedConcepts(String conceptURI) throws IOException {
	    return readConceptFieldValues(conceptURI, FIELD_RELATED);
	  }
	  
	  @Override
	  public String[] getRelatedLabels(String conceptURI) throws IOException {
	    return getLabels(conceptURI, FIELD_RELATED);
	  }
	  
	  private void indexAnnotation(Resource skos_concept, Document conceptDoc,
	      AnnotationProperty property, String field) {
	    StmtIterator stmt_iter = skos_concept.listProperties(property);
	    while (stmt_iter.hasNext()) {
	      Literal labelLiteral = stmt_iter.nextStatement().getObject()
	          .as(Literal.class);
	      String label = labelLiteral.getLexicalForm();
	      String labelLang = labelLiteral.getLanguage();
	      
	      if (this.languages != null && !this.languages.contains(labelLang)) {
	        continue;
	      }
	      
	      // converting label to lower-case
	      label = label.toLowerCase();
	      
	      Field labelField = new Field(field, label, StringField.TYPE_STORED);
	      
	      conceptDoc.add(labelField);
	    }
	  }
	  
	  private void indexObject(Resource skos_concept, Document conceptDoc,
	      ObjectProperty property, String field) {
	    StmtIterator stmt_iter = skos_concept.listProperties(property);
	    while (stmt_iter.hasNext()) {
	      RDFNode concept = stmt_iter.nextStatement().getObject();
	      
	      if (!concept.canAs(Resource.class)) {
	        System.err.println("Error when indexing relationship of concept "
	            + skos_concept.getURI() + ".");
	        continue;
	      }
	      
	      Resource resource = concept.as(Resource.class);
	      
	      String uri = resource.getURI();
	      if (uri == null) {
	        System.err.println("Error when indexing relationship of concept "
	            + skos_concept.getURI() + ".");
	        continue;
	      }
	      
	      Field conceptField = new Field(field, uri, StringField.TYPE_STORED);
	      
	      conceptDoc.add(conceptField);
	    }
	  }
	  
	  /**
	   * Creates the synonym index
	   * 
	   * @throws IOException
	   */
	  private void indexSKOSModel() throws IOException {
	    IndexWriterConfig cfg = new IndexWriterConfig(matchVersion, analyzer);
	    IndexWriter writer = new IndexWriter(indexDir, cfg);
	    writer.getConfig().setRAMBufferSizeMB(48);
	    
	    /* iterate SKOS concepts, create Lucene docs and add them to the index */
	    ResIterator concept_iter = skosModel.listResourcesWithProperty(RDF.type,
	        SKOS.Concept);
	    while (concept_iter.hasNext()) {
	      Resource skos_concept = concept_iter.next();
	      
	      Document concept_doc = createDocumentsFromConcept(skos_concept);
	      if (concept_doc != null) {
	        writer.addDocument(concept_doc);
	      }
	    }
	    
	    writer.close();
	  }
	  
	  /** Returns the values of a given field for a given concept */
	  protected String[] readConceptFieldValues(String conceptURI, String field)
	      throws IOException {
	    
	    Query query = new TermQuery(new Term(FIELD_URI, conceptURI));
	    
	    TopDocs docs = searcher.search(query, 1);
	    
	    ScoreDoc[] results = docs.scoreDocs;
	    
	    if (results.length != 1) {
	      System.out.println("Unknown concept " + conceptURI);
	      return null;
	    }
	    
	    Document conceptDoc = searcher.doc(results[0].doc);
	    
	    return conceptDoc.getValues(field);
	  }
	
	private void recursiveIndexObject(Resource skos_concept,
			Document conceptDoc, ObjectProperty property, String field) {
		
		HashSet<String> seen = new HashSet<String>();
		
		List<Resource> currentQueue = new LinkedList<Resource>();
		List<Resource> nextQueue = new LinkedList<Resource>();
		
		StmtIterator stmt_iter = skos_concept.listProperties(property);
		while (stmt_iter.hasNext()) {
			Statement statement = stmt_iter.nextStatement();
			RDFNode concept = statement.getObject();

			if (!concept.canAs(Resource.class)) {
				System.err
						.println("Error when indexing relationship of concept "
								+ skos_concept.getURI() + ".");
				continue;
			}

			Resource newResource = concept.as(Resource.class);
			
			if(!seen.contains(newResource.getURI())){
				currentQueue.add(newResource);
				seen.add(newResource.getURI());				
			}
		}
				
		int level = 1;
		
		
		while(!currentQueue.isEmpty()){
			for (Resource resource: currentQueue) {
				
				String uri = resource.getURI();
				if (uri == null) {
					System.err
							.println("Error when indexing relationship of concept "
									+ resource.getURI() + ".");
					continue;
				}
	
				Field conceptField = new Field(field+level, uri, StringField.TYPE_STORED);
	
				conceptDoc.add(conceptField);
				
				StmtIterator inner_stmt_iter = resource.listProperties(property);
				while (inner_stmt_iter .hasNext()) {
					Statement statement = inner_stmt_iter .nextStatement();
					RDFNode concept = statement.getObject();
	
					if (!concept.canAs(Resource.class)) {
						System.err
								.println("Error when indexing relationship of concept "
										+ skos_concept.getURI() + ".");
						continue;
					}
	
					Resource newResource = concept.as(Resource.class);
					
					if(!seen.contains(newResource.getURI())){
						nextQueue.add(newResource);
						seen.add(newResource.getURI());				
					}
					
				}
			}
			currentQueue.clear();
			currentQueue.addAll(nextQueue);
			nextQueue.clear();
			level++;
		}
	}
	
	@Override
	public String[] getLeveledBroaderTermsURI(String conceptURI, int level)
			throws IOException {
		return getLabels(conceptURI, FIELD_BROADER+level);
	}
	
	@Override
	public String[] getLeveledNarrowerTermsURI(String conceptURI, int level)
			throws IOException {
		return getLabels(conceptURI, FIELD_NARROWER+level);
	}
	
	@Override
	public String[] getLeveledBroaderTermsLabels(String conceptURI, int level)
			throws IOException {
		return getLabels(conceptURI, FIELD_BROADER+level);
	}
	
	@Override
	public String[] getLeveledNarrowerTermsLabels(String conceptURI, int level)
			throws IOException {
		return getLabels(conceptURI, FIELD_NARROWER+level);
	}
	

}
