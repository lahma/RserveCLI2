﻿//-----------------------------------------------------------------------
// Original work Copyright (c) 2011, Oliver M. Haynold
// Modified work Copyright (c) 2013, Suraj Gupta
// All rights reserved.
//-----------------------------------------------------------------------

using System;
using System.Text;

namespace RserveCLI2
{
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// An array of double-precision floating-point values
    /// </summary>
    /// <remarks>
    /// Uses double.NaN to represent NA
    /// </remarks>
    [Serializable]
    public class SexpArrayDouble : SexpGenericList
    {

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SexpArrayDouble"/> class.
        /// </summary>
        public SexpArrayDouble()
        {
            Value = new List<double>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SexpArrayDouble"/> class.
        /// </summary>
        public SexpArrayDouble( double theValue )
        {
            Value = new List<double> { theValue };
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SexpArrayDouble"/> class.
        /// </summary>
        /// <param name="theValue">
        /// The value.
        /// </param>
        public SexpArrayDouble( IEnumerable<double> theValue )
        {
            Value = new List<double>();
            Value.AddRange( theValue );
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SexpArrayDouble"/> class.
        /// </summary>
        /// <param name="theValue">
        /// The value.
        /// </param>
        public SexpArrayDouble( double[] theValue )
        {
            Value = new List<double>(theValue);
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the values as a 2-dimensional array
        /// </summary>
        /// <remarks>
        /// This method will only work if the Sexp was originally constructed using a 2-dimensional array.
        /// </remarks>
        public override double[ , ] As2DArrayDouble
        {
            get
            {
                if ( !Attributes.ContainsKey( "dim" ) )
                {
                    throw new NotSupportedException( "Sexp does not have the dim attribute." );
                }
                if ( Rank == 2 )
                {
                    // if GetLength fails it means the user screwed around with the dim attribute
                    int rows = GetLength( 0 );
                    int cols = GetLength( 1 );
                    var result = new double[ rows , cols ];
                    for ( int row = 0 ; row < rows ; row++ )
                    {
                        for ( int col = 0 ; col < cols ; col++ )
                        {
                            result[ row , col ] = Value[ ( col * rows ) + row ];
                        }
                    }
                    return result;
                }
                throw new NotSupportedException( "Sexp does not have 2 dimension." );
            }
        }

        /// <summary>
        /// Gets as double.
        /// </summary>
        public override double AsDouble
        {
            get
            {
                if ( Value.Count == 1 )
                {
                    return Value[ 0 ];
                }
                throw new NotSupportedException( "Can only convert length 1 double." );
            }
        }

        /// <summary>
        /// Gets as an array of double.
        /// </summary>
        /// <remarks>
        /// A matrix is flattenend by columns.  So the order is: Row1Col1, Row2Col1, Row3Col1, ... , Row1Col2, Row2Col2, Row3Col2, ...
        /// </remarks>
        public override double[] AsDoubles
        {
            get
            {
                return Value.ToArray();
            }
        }

        /// <summary>
        /// Gets as int by converting contained double.
        /// </summary>
        public override int AsInt
        {
            get
            {
                if ( ( Value.Count == 1 ) && ( ( Value[ 0 ] % 1 ) == 0 ) )
                {
                    return Convert.ToInt32( Value[ 0 ] );
                }
                throw new NotSupportedException( "Can only convert length 1 double." );
            }
        }

        /// <summary>
        /// Gets as array of int.
        /// </summary>
        /// <remarks>
        /// A matrix is flattenend by columns.  So the order is: Row1Col1, Row2Col1, Row3Col1, ... , Row1Col2, Row2Col2, Row3Col2, ...
        /// </remarks>
        public override int[] AsInts
        {
            get
            {
                if ( Value.Select( x => ( x % 1 ) == 0 ).All( y => y ) )
                {
                    return Value.Select( Convert.ToInt32 ).ToArray();
                }
                throw new NotSupportedException( "Can only convert length 1 double." );
            }
        }

        /// <summary>
        /// Gets the number of elements contained in the ICollection.
        /// </summary>
        public override int Count
        {
            get
            {
                return Value.Count;
            }
        }

        /// <summary>
        /// Determines if double is NA
        /// </summary>
        public override bool IsNa
        {
            get
            {
                if ( Value.Count == 1 )
                {
                    return double.IsNaN( Value[ 0 ] );
                }
                throw new NotSupportedException( "Can only check NA for length 1 double." );
            }
        }

        /// <summary>
        /// Determines if the ICollection is read-only.
        /// </summary>
        public override bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// Gets the NA value of double.
        /// </summary>
        public static double Na
        {
            get
            {
                return double.NaN;
            }
        }

        /// <summary>
        /// Gets the doubles stored in the list
        /// </summary>
        internal List<double> Value { get; private set; }
        
        #endregion

        #region Indexers

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the element</param>
        /// <returns>
        /// The element at the specified index.
        /// </returns>
        public override Sexp this[ int index ]
        {
            get
            {
                return new SexpArrayDouble( Value[ index ] );
            }

            set
            {
                Value[ index ] = value.AsDouble;
            }
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Adds an item to the ICollection.
        /// </summary>
        public override void Add( Sexp item )
        {
            Value.AddRange( item.AsDoubles );
        }

        /// <summary>
        /// Checks whether a value is NA.
        /// </summary>
        public static bool CheckNa( double x )
        {
            return double.IsNaN( x );
        }

        /// <summary>
        /// Removes all items from the ICollection.
        /// </summary>
        public override void Clear()
        {
            Value.Clear();
        }

        /// <summary>
        /// Determines whether the ICollection contains a specific value.
        /// </summary>
        public override bool Contains( Sexp item )
        {
            return Value.Contains( item.AsDouble );
        }

        /// <summary>
        /// Copies the elements of the ICollection to an Array, starting at a particular Array index.
        /// </summary>
        /// <param name="array">The one-dimensional Array that is the destination of the elements copied from ICollection. The Array must have zero-based indexing.</param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins</param>
        public override void CopyTo( Sexp[] array , int arrayIndex )
        {
            for ( int i = 0 ; i < Value.Count ; i++ )
            {
                array[ arrayIndex + i ] = new SexpArrayDouble( Value[ i ] );
            }
        }

        /// <summary>
        /// Determines whether the specified object is equal to this instance.
        /// </summary>
        /// <param name="obj">The object to compare with this instance.</param>
        /// <returns>
        /// true if the specified object is equal to this instance; otherwise, false.
        /// Does not check for attribute equality.
        /// </returns>
        public override bool Equals( object obj )
        {
            if ( obj == null)
            {
                return false;
            }
            var objSexpArrayDouble = obj as SexpArrayDouble;
            if ( objSexpArrayDouble != null )
            {
                return Equals( objSexpArrayDouble );
            }
            
            // can obj be coersed into an array of double?
            try
            {
                return Equals( new SexpArrayDouble( Make( obj ).AsDoubles ) );
            }
            catch ( NotSupportedException ) { }
            return false;
        }

        /// <summary>
        /// Determines whether the specified SexpArrayDouble is equal to this instance.
        /// </summary>
        /// <param name="other"></param>
        /// <returns>
        /// <c>true</c> if the specified SexpArrayDouble is equal to this instance; otherwise, <c>false</c>.
        /// Does not check for attribute equality.
        /// </returns>
        public bool Equals( SexpArrayDouble other )
        {
            if ( ReferenceEquals( null , other ) ) return false;
            if ( ReferenceEquals( this , other ) ) return true;
            return other.Value.SequenceEqual( Value );
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A IEnumerator that can be used to iterate through the collection.
        /// </returns>
        public override IEnumerator<Sexp> GetEnumerator()
        {
            return ( from a in Value select ( Sexp )( new SexpArrayDouble( a ) ) ).GetEnumerator();
        }

        /// <summary>
        /// Return hash code for this instance.
        /// </summary>
        public override int GetHashCode()
        {
            unchecked
            {
                return ( base.GetHashCode() * 397 ) ^ ( Value != null ? Value.GetHashCode() : 0 );
            }
        }

        /// <summary>
        /// Determines the index of a specific item in the IList.
        /// </summary>
        /// <param name="item">The object to locate in the IList.</param>
        /// <returns>
        /// The index of item if found in the list; otherwise, -1.
        /// </returns>
        public override int IndexOf( Sexp item )
        {
            return Value.IndexOf( item.AsDouble );            
        }

        /// <summary>
        /// Inserts an item to the IList at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which item should be inserted.</param>
        /// <param name="item">The object to insert into the IList.</param>
        public override void Insert( int index , Sexp item )
        {
            Value.Insert( index , item.AsDouble );
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the ICollection.
        /// </summary>
        /// <param name="item">The object to remove from the ICollection.</param>
        /// <returns>
        /// true if item was successfully removed from the ICollection; otherwise, false. This method also returns false if item is not found in the original ICollection.
        /// </returns>
        public override bool Remove( Sexp item )
        {
            return Value.Remove( item.AsDouble );
        }

        /// <summary>
        /// Removes the IList item at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to remove.</param>
        public override void RemoveAt( int index )
        {
            Value.RemoveAt( index );
        }

        /// <summary>
        /// Converts the Sexp into the most appropriate native representation. 
        /// Use with caution--this is more a rapid prototyping than a production feature.
        /// </summary>
        /// <returns>
        /// A CLI native representation of the Sexp
        /// </returns>
        public override object ToNative()
        {
            return Value.ToArray();
        }

        /// <summary>
        /// Returns a string that represents this instance.
        /// </summary>
        /// <returns>
        /// A string that represents this instance.
        /// </returns>
        public override string ToString()
        {
            var builder = new StringBuilder();
            foreach ( double value in Value )
            {
                builder.Append( " " );
                builder.Append( CheckNa( value ) ? "NA" : value.ToString() );
            }
            if ( builder.Length > 0 )
            {
                builder.Remove( 0 , 1 );
            }
            return builder.ToString();
        }
        
        #endregion        
    }
}
