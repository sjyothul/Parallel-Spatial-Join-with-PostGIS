# Parallel-Spatial-Join-with-PostGIS

We load the points and rectangles datasets into two PostgreSQL tables: points and rectangles respectively.
The points table has a geometry type column geom which denotes Point geometry.
The rectangles table has a geometry type column geom which denotes Envelop geometry (rectangle).
The high level objective is to find the number of points inside each rectangle, save it to the outputTable and 
outputPath. 
Perform the following tasks:
• Create four partitions/fragments of both pointsTable and rectsTable. We should consider space 
partitioning such that all points or rectangles of a partition should lie within the corresponding 
fragment. Fragments doesn’t need to satisfy disjointness property.
• Run four parallel threads. Each thread should perform a spatial join between a fragment of
pointsTable and a fragment of rectsTable. The purpose of the join is to find the number of points 
(pointsTable.geom) inside each rectangle or Envelop (rectsTable.geom) within the corresponding 
fragment. You must make use of ST_Contains method supported by PostGIS.
• Sort the output of each fragment in the ascending order of counts of points inside the parallel 
threads.
• Merge the outputs of four parallel joins into outputTable in the ascending order of counts of points. 
You can design the structure of the outputTable as you wish, but it should have a column named 
points_count containing counts of points.
• Write the counts of points into the outputPath in the ascending order. You don’t need to write 
rectangle coordinates.
