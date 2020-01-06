# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 14:30:25 2019

@author: karaf
"""

import ogr

"""Define a geometric point and a few common manipulations."""
class Point( object ):
    """A 2-D geometric point."""
    def __init__( self, x, y ):
        """Create a point at (x,y)."""
        self.x, self.y = x, y
    def offset( self, xo, yo ):
        """Offset the point by xo parallel to the x-axis
        and yo parallel to the y-axis."""
        self.x += xo
        self.y += yo
    def offset2( self, val ):
        """Offset the point by val parallel to both axes."""
        self.offset( val, val )
    def __str__( self ):
        """Return a pleasant representation."""
        return "(%g,%g)" % ( self.x, self.y )
    def transform( self, fromEPSG, toEPSG ):
        ## EPSG4326: WGS84 geo - EPSG31468: UTM32N datum WGS84

        #original SRS
        oSRS=ogr.osr.SpatialReference()
        oSRS.ImportFromEPSG(fromEPSG)

        #target SRS
        tSRS=ogr.osr.SpatialReference()
        tSRS.ImportFromEPSG(toEPSG)

        coordTrans=ogr.osr.CoordinateTransformation(oSRS,tSRS)
        res=coordTrans.TransformPoint(self.x, self.y, 0.)
        self.x = res[0]
        self.y = res[1]
        
        
P1 = Point(17.98051, 44.623534)
print(P1.x)
print(P1.y)
P1.transform(4326, 32632)
print(P1.x)
print(P1.y)


        
        