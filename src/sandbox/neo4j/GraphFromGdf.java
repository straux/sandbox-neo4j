package sandbox.neo4j;

import sandbox.neo4j.ReadGDF;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Iterator;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class GraphFromGdf {

    public enum MyRelTypes implements RelationshipType {
        LINKS
    }
    public static final long MAX_NB_TX = 100000;

    public static GraphDatabaseService loadGDF( String gdf, String base ) {
    //public static void loadGDF( String gdf, String base ) {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( base );

        ReadGDF reader = new ReadGDF( gdf );
        Hashtable nodes = new Hashtable();
        
        int check = 1;
        while( check > 0 && reader.hasNextLine() ) {
            long i = 0;
            Transaction tx = graphDb.beginTx();
            try {
                while( i++ < MAX_NB_TX && check > 0 && reader.hasNextLine() ) {
                    Hashtable fields = reader.get_node();
                    check = fields.size();
                    if( check > 0 ) {
                        Node node = graphDb.createNode();
                        nodes.put( fields.remove("name"), node.getId() );

                        Enumeration e = fields.keys(); 
                        while( e.hasMoreElements() ) {
                            String key = (String) e.nextElement();
                            node.setProperty( key, fields.get( key ) );
                        }
                    }
                }
                tx.success();
            } catch ( Exception e ) {
                System.err.println( e );   
            } finally {
                tx.finish();    
            }
        }

        while( reader.hasNextLine() ) {
            long i = 0;
            Transaction tx = graphDb.beginTx();
            try {
                while( i++ < MAX_NB_TX && reader.hasNextLine() ) {
                    Hashtable fields = reader.get_edge();
                    String key1 = (String) fields.remove( "node1" );
                    String key2 = (String) fields.remove( "node2" );
                    if( key1 != null && key2 != null && !key1.equals( key2 ) ) {
                        Long id1 = (Long) nodes.get( key1 );
                        Long id2 = (Long) nodes.get( key2 );
                        Node from = graphDb.getNodeById( id1 );
                        Node to   = graphDb.getNodeById( id2 );

                        Relationship relationship = from.createRelationshipTo( to, MyRelTypes.LINKS );
                        Enumeration e = fields.keys(); 
                        while( e.hasMoreElements() ) {
                            String key = (String) e.nextElement();
                            relationship.setProperty( key, fields.get( key ) );
                        }
                    }
                }

                tx.success();
            } catch ( Exception e ) {
                System.err.println( e );   
            } finally {
                tx.finish();    
            }
        }
        return graphDb;
    }

    public static void main(String[] args) {

        if( args.length == 0 ) {
            System.err.println( "Args: file.gdf [base_path=var/base]");
        } else {

            String gdfFile = args[0];
            String base = "var/base";
            if( args.length > 1 ) {
                base = args[1];
            }
            
             GraphDatabaseService graph = loadGDF( gdfFile, base );
             graph.shutdown();
        }
    }
}
