﻿using System.Collections.Generic;
using Xunit;

namespace RserveCLI2.Test
{
    public class SexpListTest
    {

        [Fact]
        public void Various_ListTest()
        {
            // note: this test was ported from RserveCLI.  Not in the typical Arrange/Act/Assert format
            using ( var service = new Rservice() )
            {
                var mylist = new Dictionary<string , object> { { "One" , 1 } , { "Two" , 2.0 } , { "Three" , "three" } };
                var x1 = Sexp.Make( mylist );
                service.RConnection[ "x1" ] = x1;

                Assert.Equal( x1.Count , mylist.Count );

                service.RConnection.Eval( "x2 <- list(One=1,Two=2.0,Three=\"three\")" );
                var x2 = service.RConnection[ "x2" ];

                Assert.Equal( x1.Count , x2.Count );

                for ( int i = 0 ; i < x1.Count ; i++ )
                {
                    Assert.True( x1[ i ].Equals( x2[ i ] ) );
                    Assert.Equal( x1.Names[ i ].AsString , x2.Names[ i ].AsString );
                    Assert.True( x1[ x1.Names[ i ].AsString ].Equals( x2[ x2.Names[ i ].AsString ] ) );
                }
            }
        }
        
    }
}
