package at.ac.univie.mminf.luceneSKOS.skos;

import java.io.IOException;
import java.util.List;

public interface MeSHEngine extends SKOSEngine {
		
	public String[] getLeveledBroaderTermsURI(String conceptURI, int level) throws IOException;
	public String[] getLeveledNarrowerTermsURI(String conceptURI, int level) throws IOException;
	
	public String[] getLeveledBroaderTermsLabels(String conceptURI, int level) throws IOException;
	public String[] getLeveledNarrowerTermsLabels(String conceptURI, int level) throws IOException;
	
}
