package sandbox.neo4j;

import sandbox.neo4j.ReadGDF;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Iterator;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Example class that iterates over nodes and relationships
 */
public class GraphSize {

    public enum MyRelTypes implements RelationshipType {
        LINKS
    }

    public static void main(String[] args) {

        String base = "var/base";
        if( args.length > 0 ) {
            base = args[0];
        }
        
        System.err.println("loading graph from base " + base);
        GraphDatabaseService graph = new EmbeddedGraphDatabase( base );
        
        long nb_nodes = 0;
        long nb_edges = 0;
         Iterator<Node> it = graph.getAllNodes().iterator();
         it.next(); // skip root node
         while( it.hasNext() ) {
            Node node = it.next();
            nb_nodes++;

            Iterator<Relationship> rel = node.getRelationships( Direction.OUTGOING ).iterator();
            while( rel.hasNext() ) {
                rel.next();
                nb_edges++;
            }
         }
         System.out.println("nodes: " + nb_nodes );
         System.out.println("relationships: " + nb_edges );
         graph.shutdown();
    }
}
