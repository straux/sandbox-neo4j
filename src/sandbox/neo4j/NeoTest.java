package sandbox.neo4j;
 
import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class NeoTest {
 
    public enum MyRelationshipTypes implements RelationshipType {
        KNOWS
    }
 
    public static void main(String[] args) {
        
        String base = "var/base";
        if( args.length > 0 ) {
            base = args[0];
        }

        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( base );
 
        Transaction tx = graphDb.beginTx();
        try {
            Node firstNode = graphDb.createNode();
            Node secondNode = graphDb.createNode();
            Relationship relationship = firstNode.createRelationshipTo(secondNode, MyRelationshipTypes.KNOWS);
 
            firstNode.setProperty("message", "Hello, ");
            secondNode.setProperty("message", "world!");
            relationship.setProperty("message", "brave Neo4j ");
            tx.success();
 
            System.out.print(firstNode.getProperty("message"));
            System.out.print(relationship.getProperty("message"));
            System.out.println(secondNode.getProperty("message"));
        }
        finally {
            tx.finish();
            graphDb.shutdown();
        }
    }
}
