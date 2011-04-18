package sandbox.neo4j;

import sandbox.neo4j.ReadGDF;
import java.util.Hashtable;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class GraphFromGdf {

    public enum MyRelTypes implements RelationshipType {
        LINKS
    }

    //public static GraphDatabaseService loadGDF( String gdf, String base ) {
    public static void loadGDF( String gdf, String base ) {
        //GraphDatabaseService graphDb = new EmbeddedGraphDatabase( base );

        ReadGDF reader = new ReadGDF( gdf );
        int check = 1;
        while( check > 0 && reader.hasNextLine() ) {
            Hashtable fields = reader.get_node();
            check = fields.size();
            if( check > 0 ) {
                System.out.println("Node:" + fields.get( "name" ) );
            }
        }
        while( reader.hasNextLine() ) {
            Hashtable fields = reader.get_edge();
            System.out.println("Edge:" + fields.get( "node1" ) + " => " + fields.get( "node2" ) );
        }
        //return graphDb;
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

            loadGDF( gdfFile, base );
            //Transaction tx = graphDb.beginTx();
            //try {} finally {
                //tx.finish();
                //graphDb.shutdown();
            //}
            //GraphDatabaseService graphDb = new EmbeddedGraphDatabase( base );

            //Transaction tx = graphDb.beginTx();
            //try {
                //Node firstNode = graphDb.createNode();
                //Node secondNode = graphDb.createNode();
                //Relationship relationship = firstNode.createRelationshipTo(secondNode, MyRelTypes.LINKS );

                //firstNode.setProperty("message", "Hello, ");
                //secondNode.setProperty("message", "world!");
                //relationship.setProperty("message", "brave Neo4j ");
                //tx.success();

                //System.out.print(firstNode.getProperty("message"));
                //System.out.print(relationship.getProperty("message"));
                //System.out.println(secondNode.getProperty("message"));
            //}
            //finally {
                //graphDb.shutdown();
            //}
        }
    }
}
